# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_hhs`.
"""
from datetime import date, datetime, timedelta
from itertools import product

import time
from delphi_epidata import Epidata
from delphi_utils.export import create_export_csv
from delphi_utils.geomap import GeoMapper
from delphi_utils import get_structured_logger
import numpy as np
import pandas as pd

from .constants import SIGNALS, GEOS, SMOOTHERS, CONFIRMED, SUM_CONF_SUSP

def _date_to_int(d):
    """Return a date object as a yyyymmdd int."""
    return int(d.strftime("%Y%m%d"))


def int_date_to_previous_day_datetime(x):
    """Convert integer dates to Python datetimes for the previous day.

    Epidata uses an integer date format. This needs to be converted to
    a datetime so that the exporter can interpret it.

    The HHS columns we are interested in measure admissions for the
    previous day. To accurately indicate the date of incidence, the
    date must be shifted back by one day.
    """
    return x.apply(lambda x: datetime.strptime(str(x), "%Y%m%d") - timedelta(days=1))


def generate_date_ranges(start, end):
    """
    Take a start and end date and convert to list of 30 day Epidata ranges.

    The final tuple may only be a few days depending of the modulo of the range and 30.
    The ranges should partition the entire range, inclusive of both endpoints, and do not overlap,
    i.e. they will be of the form (start, start+30), (start+31, start+61), (start+62, start+92), ...

    Parameters
    ----------
    start: date
      datetime.date object for first day.
    end: datetime
      datetime.date object for last day.

    Returns
    -------
    Ordered list of dictionaries generated by Epidata.range specifying the partitioning intervals.
    """
    curr_end = start + timedelta(30)
    output = []
    while curr_end < end:
        output.append(Epidata.range(_date_to_int(start), _date_to_int(curr_end)))
        start += timedelta(31)
        curr_end = start + timedelta(30)
    output.append(Epidata.range(_date_to_int(start), _date_to_int(end)))
    return output


def run_module(params):
    """
    Generate ground truth HHS hospitalization data.

    Parameters
    ----------
    params
        Dictionary containing indicator configuration. Expected to have the following structure:
        - "common":
            - "export_dir": str, directory to write output
            - "log_filename" (optional): str, name of file to write logs
            - "epidata" (optional): dict, extra parameters to send to Epidata.covid_hosp
    """
    start_time = time.time()
    logger = get_structured_logger(
        __name__, filename=params["common"].get("log_filename"),
        log_exceptions=params["common"].get("log_exceptions", True))
    mapper = GeoMapper()
    request_all_states = ",".join(mapper.get_geo_values("state_id"))
    end_day = date.today()
    if "epidata" in params["common"] and \
       "as_of" in params["common"]["epidata"]:
        end_day = min(
            end_day,
            datetime.strptime(str(params["common"]["epidata"]["as_of"]), "%Y%m%d").date()
        )
    past_reference_day = date(year=2020, month=1, day=1)  # first available date in DB
    date_range = generate_date_ranges(past_reference_day, end_day)
    dfs = []
    for r in date_range:
        response = Epidata.covid_hosp(request_all_states, r, **params["common"].get("epidata", {}))
        # The last date range might only have recent days that don't have any data, so don't error.
        if response["result"] != 1 and r != date_range[-1]:
            raise Exception(f"Bad result from Epidata for {r}: {response['message']}")
        if response["result"] == -2 and r == date_range[-1]:  # -2 code means no results
            continue
        dfs.append(pd.DataFrame(response['epidata']))
    all_columns = pd.concat(dfs)
    geo_mapper = GeoMapper()
    stats = []
    for sensor, smoother, geo in product(SIGNALS, SMOOTHERS, GEOS):
        logger.info("Generating signal and exporting to CSV",
                    geo_res = geo,
                    sensor = sensor,
                    smoother = smoother)
        df = geo_mapper.add_geocode(make_signal(all_columns, sensor),
                                    "state_id",
                                    "state_code",
                                    from_col="state")
        if sensor.endswith("_prop"):
            df=pop_proportion(df, geo_mapper)
        df = make_geo(df, geo, geo_mapper)
        df = smooth_values(df, smoother[0])
        if df.empty:
            continue
        sensor_name = sensor + smoother[1]
        # don't export first 6 days for smoothed signals since they'll be nan.
        start_date = min(df.timestamp) + timedelta(6) if smoother[1] else min(df.timestamp)
        dates = create_export_csv(df,
                          params["common"]["export_dir"],
                          geo,
                          sensor_name,
                          start_date=start_date)
        if len(dates) > 0:
            stats.append((max(dates), len(dates)))

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    min_max_date = stats and min(s[0] for s in stats)
    csv_export_count = sum(s[-1] for s in stats)
    max_lag_in_days = min_max_date and (datetime.now() - min_max_date).days
    formatted_min_max_date = min_max_date and min_max_date.strftime("%Y-%m-%d")
    logger.info("Completed indicator run",
                elapsed_time_in_seconds = elapsed_time_in_seconds,
                csv_export_count = csv_export_count,
                max_lag_in_days = max_lag_in_days,
                oldest_final_export_date = formatted_min_max_date)


def smooth_values(df, smoother):
    """Smooth the value column in the dataframe."""
    df["val"] = df["val"].astype(float)
    df["val"] = df[["geo_id", "val"]].groupby("geo_id")["val"].transform(
        smoother.smooth
    )
    return df

def pop_proportion(df,geo_mapper):
    """Get the population-proportionate variants as the dataframe val."""
    pop_val=geo_mapper.add_population_column(df, "state_code")
    df["val"]=round(df["val"]/pop_val["population"]*100000, 7)
    pop_val.drop("population", axis=1, inplace=True)
    return df

def make_geo(state, geo, geo_mapper):
    """Transform incoming geo (state) to another geo."""
    if geo == "state":
        exported = state.rename(columns={"state": "geo_id"})
    else:
        exported = geo_mapper.replace_geocode(state, "state_code", geo, new_col="geo_id")
    exported["se"] = np.nan
    exported["sample_size"] = np.nan
    return exported


def make_signal(all_columns, sig):
    """Generate column sums according to signal name."""
    assert sig in SIGNALS, f"Unexpected signal name '{sig}';" + \
        " familiar names are '{', '.join(SIGNALS)}'"
    if sig.startswith(CONFIRMED):
        df = pd.DataFrame({
            "state": all_columns.state.apply(str.lower),
            "timestamp":int_date_to_previous_day_datetime(all_columns.date),
            "val": \
            all_columns.previous_day_admission_adult_covid_confirmed + \
            all_columns.previous_day_admission_pediatric_covid_confirmed
        })
    elif sig.startswith(SUM_CONF_SUSP):
        df = pd.DataFrame({
            "state": all_columns.state.apply(str.lower),
            "timestamp":int_date_to_previous_day_datetime(all_columns.date),
            "val": \
            all_columns.previous_day_admission_adult_covid_confirmed + \
            all_columns.previous_day_admission_adult_covid_suspected + \
            all_columns.previous_day_admission_pediatric_covid_confirmed + \
            all_columns.previous_day_admission_pediatric_covid_suspected,
        })
    else:
        raise Exception(
            "Bad programmer: signal '{sig}' in SIGNALS but not handled in make_signal"
        )
    df["val"] = df.val.astype(float)
    return df
