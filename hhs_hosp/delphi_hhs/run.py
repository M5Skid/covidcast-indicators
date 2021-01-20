# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_hhs`.
"""
from datetime import date, datetime, timedelta

from delphi_epidata import Epidata
from delphi_utils import read_params
from delphi_utils.export import create_export_csv
from delphi_utils.geomap import GeoMapper
import numpy as np
import pandas as pd

from .constants import SIGNALS, GEOS, CONFIRMED, SUM_CONF_SUSP


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


def run_module():
    """Generate ground truth HHS hospitalization data."""
    params = read_params()
    mapper = GeoMapper()
    request_all_states = ",".join(mapper.get_geo_values("state_id"))

    today = date.today()
    past_reference_day = date(year=2020, month=1, day=1) # first available date in DB
    date_range = generate_date_ranges(past_reference_day, today)
    dfs = []
    for r in date_range:
        response = Epidata.covid_hosp(request_all_states, r)
        if response['result'] != 1:
            raise Exception(f"Bad result from Epidata: {response['message']}")
        dfs.append(pd.DataFrame(response['epidata']))
    all_columns = pd.concat(dfs)

    geo_mapper = GeoMapper()

    for sig in SIGNALS:
        state = geo_mapper.add_geocode(make_signal(all_columns, sig),
                                       "state_id", "state_code",
                                       from_col="state")
        for geo in GEOS:
            if geo == "state":
                exported = state.rename(columns={"state":"geo_id"})
            else:
                exported = geo_mapper.replace_geocode(
                    state, "state_code", geo,
                    new_col="geo_id",
                    date_col="timestamp")
            exported["se"] = np.nan
            exported["sample_size"] = np.nan
            try:
                create_export_csv(
                    exported,
                    params["export_dir"],
                    geo,
                    sig
                )
            except (KeyError,ValueError) as e:
                print(geo)
                print(state)
                print(exported)
                raise

def make_signal(all_columns, sig):
    """Generate column sums according to signal name."""
    assert sig in SIGNALS, f"Unexpected signal name '{sig}';" + \
        " familiar names are '{', '.join(SIGNALS)}'"
    if sig == CONFIRMED:
        return pd.DataFrame({
            "state": all_columns.state.apply(str.lower),
            "timestamp":int_date_to_previous_day_datetime(all_columns.date),
            "val": \
            all_columns.previous_day_admission_adult_covid_confirmed + \
            all_columns.previous_day_admission_pediatric_covid_confirmed
        })
    if sig == SUM_CONF_SUSP:
        return pd.DataFrame({
            "state": all_columns.state.apply(str.lower),
            "timestamp":int_date_to_previous_day_datetime(all_columns.date),
            "val": \
            all_columns.previous_day_admission_adult_covid_confirmed + \
            all_columns.previous_day_admission_adult_covid_suspected + \
            all_columns.previous_day_admission_pediatric_covid_confirmed + \
            all_columns.previous_day_admission_pediatric_covid_suspected,
        })
    raise Exception(
        "Bad programmer: signal '{sig}' in SIGNALS but not handled in make_signal"
    )
