import argparse
from contextlib import redirect_stdout
import datetime as dt
import filecmp
import json
import os
from os.path import join
import pdb
from pprint import pprint

import sys
import run


import shutil
import traceback
from typing import Tuple

from boto3 import Session
from moto import mock_s3
import pandas as pd
import numpy as np

from delphi_utils.archive import diff_export_csv

def load_csv(csv_file: str) -> pd.DataFrame:
    df = pd.read_csv(
        csv_file,
        parse_dates=["issue_date", "time_value"],
        dtype={
            "metric": str,
            "UID": str,
            "fips": str,
            "combined_key": str,
            "value_from": int,
            "value_to": int,
            "from_git_hash": str,
            "to_git_hash": str
        })

    # Try to recover FIPS from UID
    invalid_fips = df["fips"].isin(["0", "00000"]) | df["fips"].isnull()
    df.loc[invalid_fips, "fips"] = df.loc[invalid_fips, "UID"].str[3, 8]
    df["fips"].replace("", np.nan, inplace=True)

    return df

def load_issues_git_refs(csv_file: str) -> pd.DataFrame:
    df_issues = pd.read_csv(
        csv_file,
        parse_dates=["issue_date"],
        dtype={
            "metrics": str,
            "from_git_hash": str,
            "to_git_hash": str
        })

    return df_issues

def run_pipeline(
        git_ref: str,
        output_dir: str,
        template_params: str = "params.json.template",
        cached: bool = True,
        redir_stdout: bool = False) -> Tuple[str, set]:

    print(f"Running pipeline on data version '{git_ref}'")
    cache_dir = join(output_dir, f"cache_{git_ref}")
    export_dir = join(output_dir, f"receiving_{git_ref}")
    logging_file = join(output_dir, f"run_module_{git_ref}.log")

    # Make sure directories exist
    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(export_dir, exist_ok=True)

    # Either ignoring cache or export directory empty
    if not cached or not os.listdir(export_dir):

        with open(template_params, "r") as f_json:
            params = json.load(f_json)

        # Set up the params for specified git ref
        params["common"]["export_dir"] = export_dir
        params["indicator"]["base_url"] = f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/{git_ref}/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_{{metric}}_US.csv"
        params["aws_credentials"] = {
            "aws_access_key_id": "FAKE_TEST_ACCESS_KEY_ID",
            "aws_secret_access_key": "FAKE_TEST_SECRET_ACCESS_KEY"
        }
        params["bucket_name"] = "test-bucket"

        with open("params.json", "w") as f_json:
            json.dump(params, f_json)

        def run_mocked():
            try:
                # Mock S3 interactions
                with mock_s3():
                    s3_client = Session(**params["aws_credentials"]).client("s3")
                    s3_client.create_bucket(Bucket=params["bucket_name"])

                    run.run_module(params)
            except (Exception, KeyboardInterrupt) as ex:
                # Pipeline crashed, exports probably not complete
                # Rename it to investigate and prevent caching incomplete output
                os.rename(export_dir, f"{export_dir}_partial")
                raise ex
            finally:
                sys.stdout.flush()

        # Redirect STDOUT output into a file instead
        if redir_stdout:
            with open(logging_file, "w") as f_log:
                with redirect_stdout(f_log):
                    run_mocked()
        else:
            run_mocked()

    output_files = set(os.listdir(export_dir))
    print(export_dir)
    print(os.listdir(export_dir))
    shutil.rmtree(cache_dir)

    return export_dir, output_files

def parse_signal_info(csv_file: str):
    filename, _ = os.path.splitext(os.path.basename(csv_file))
    splits = filename.split("_")
    time_value = splits[0]
    geo_type = splits[1]
    signal = "_".join(splits[2:])

    return time_value, geo_type, signal

def create_issues(
        issue_date: dt.datetime,
        from_git_hash: str,
        to_git_hash: str,
        tmp_dir: str,
        output_dir: str,
        redir_stdout: bool = False):

    prev_date_str = (issue_date - dt.timedelta(days=1)).strftime("%Y%m%d")
    issue_date_str = issue_date.strftime("%Y%m%d")

    csv_dir = join(output_dir, f"issue_{issue_date_str}", "jhu")
    empty_from = from_git_hash is None or pd.isnull(from_git_hash)
    empty_to = to_git_hash is None or pd.isnull(to_git_hash)
    assert not empty_to, f"to_git_hash cannot be '{to_git_hash}'"

    if not empty_from:
        try:
            before_export_dir, before_files = run_pipeline(from_git_hash, tmp_dir, redir_stdout=redir_stdout)
        except KeyboardInterrupt as ex:
            raise ex
        except Exception as ex:
            print(f"Running on hash {from_git_hash} crashed")
            traceback.print_exc(file=sys.stdout)
            sys.stdout.flush()
            return

    try:
        after_export_dir, after_files = run_pipeline(to_git_hash, args.tmp, redir_stdout=redir_stdout)
    except KeyboardInterrupt as ex:
        raise ex
    except Exception as ex:
        print(f"Running on hash {to_git_hash} crashed")
        traceback.print_exc(file=sys.stdout)
        sys.stdout.flush()
        return

    if empty_from:

        # Copy all output from after_export_dir to csv_dir
        os.makedirs(csv_dir, exist_ok=True)
        for output_file in after_files:
            shutil.copy(
                join(after_export_dir, output_file),
                csv_dir)

        return

    print(after_files)
    print("----------------------")
    # Check 'before' files are a subset of 'after' files
    if before_files <= after_files:
        after_files_excl_new = {
            f for f in after_files
            if parse_signal_info(f)[0] not in (prev_date_str, issue_date_str)
        }
        if before_files < after_files_excl_new:
            print(f"For issue date {issue_date_str}, extra output from 'after' version of data, verify if intended:")
            pprint([
                parse_signal_info(f)
                for f in after_files_excl_new - before_files])

        if os.path.exists(csv_dir):
            print(f"Skipping existing results {csv_dir}")
        else:
            os.makedirs(csv_dir, exist_ok=True)
            new_files = after_files - before_files
            common_files = before_files & after_files

            print(f"Creating issues between {from_git_hash} -> {to_git_hash}")
            try:
                # Copy over new files
                for output_file in new_files:
                    shutil.copy(
                        join(after_export_dir, output_file),
                        csv_dir)

                # Common files need diffing
                for output_file in common_files:
                    before_file = join(before_export_dir, output_file)
                    after_file = join(after_export_dir, output_file)
                    issue_file = join(csv_dir, output_file)

                    # Check for simple file similarity before doing CSV diffs
                    if filecmp.cmp(before_file, after_file, shallow=False):
                        continue

                    deleted_df, changed_df, added_df = diff_export_csv(
                        before_file,
                        after_file)
                    new_issues_df = pd.concat([changed_df, added_df], axis=0)

                    if len(deleted_df) > 0:
                        print(
                            f"Warning, diff has deleted indices in {after_file} that will be ignored")
                    if len(new_issues_df) > 0:
                        new_issues_df.to_csv(issue_file, na_rep="NA")

            except (Exception, KeyboardInterrupt) as ex:
                # Diffing crashed, CSV outputs probably not complete
                # Rename it to investigate and prevent caching incomplete output
                os.rename(
                    join(output_dir, f"issues_{issue_date_str}"),
                    join(output_dir, f"issues_{issue_date_str}_partial"))
                raise ex
    else:
        print("Some output that were present in '{from_git_hash}' are not present in '{to_git_hash}'!")
        pprint(before_files - after_files)
        pdb.set_trace()


def main(args):
    # df_errata = load_csv(args.errata_file)
    df_issues = load_issues_git_refs(args.issues_file)

    os.makedirs(args.out, exist_ok=True)
    for entry in df_issues.itertuples(index=False):
        create_issues(
            entry.issue_date,
            entry.from_git_hash,
            entry.to_git_hash,
            args.tmp,
            args.out,
            redir_stdout=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("issues_file", help="Issue git refs from Google sheets")
    parser.add_argument(
        "--tmp", dest="tmp", default="tmp",
        help="Temporary directory to store pipeline results")
    parser.add_argument(
        "--out", dest="out", default="output",
        help="Output directory to store resulting SQL files")
    args = parser.parse_args()

    main(args)
