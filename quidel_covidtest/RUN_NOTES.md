# Quidel Covidtest Notes
This is a working document designed to compile information into a user friendly FAQ style guide. 

## Intro
Quidel sources information based on covid antigen test data. Signal data consists of percentages of antigen tests that were positive for covid.

### Signal Breakdown
- Most in the form of `covid_ag_...` because it is the same statistic across different groups.
- Ex: `covid_ag_raw_pct_positive_age_18_49`
- Split up by various age and geographic groups.
- Both a smoothed and raw signal are created for each group.

### Data Collection
- Python module connects to AWS. 
- Requires AWS access key and ID (Separate from archive credentials).
- [Question] How do we connect to it? Is this on Delphi's server?

## Running the Indicator
The module name and venv/makefile information should be in the `README.md`. Provided the parameters are specified and the necessary packages installed, the indicator can be run in a local environment.
- Run time is dependent on the length of pull/export window, which should be kept to a minimum
- If ran with `delphi_utils.runner`, validator and archive will also add time to the run.

### Directory Structure
The `quidel_covidtest/` indicator directory is expected to have the following subdirectories and files. Many can be overwritten in `params.json` (see below).

Run Module
- `delphi_quidel_covidtest/` - Contains indicator code.
- [Thought] A simple code breakdown would be great, but a massive timesink.

Functional Directories/Files
- `backfill/` - Backfill dir for parquet files.
- `cache/` - Input cache for pulled data.
- `receiving/` - Contains csv export files.
- `logs/quidel-covidtest.log` - Module log printed by structured logger. Will contain additional logging if delphi_utils functions are run on the indicator (such as validator).
- `params.json` and `params.json.template` - user set parameters

### Parameter Notes
Here is the code breakdown from `run.py` with additional parameters added:
```
The `params` argument is expected to have the following structure:

"common":
  "export_dir": str, directory to write output
  "log_exceptions" (optional): bool, whether to log exceptions to file
  "log_filename" (optional): str, name of file to write logs

"indicator":
  "static_file_dir": str, directory name with population information
  "input_cache_dir": str, directory in which to cache input data
  "backfill_dir": str, directory in which to store the backfill files
  "export_start_date": str, YYYY-MM-DD format of earliest date to create output
  "export_end_date": str, YYYY-MM-DD format of latest date to create output or "" to create through the present
  "pull_start_date": str, YYYY-MM-DD format of earliest date to pull input
  "pull_end_date": str, YYYY-MM-DD format of latest date to create output or "" to create through the present
  "export_day_range": int, number of date range
  "aws_credentials": Dict[str, str], authentication parameters for AWS S3; see S3      documentation
  "bucket_name": str, name of AWS bucket in which to find data
  "wip_signal": List[str], list of signal names that are works in progress
  "test_mode": bool, whether we are running in test mode

"delivery":
  "delivery_dir": str, directory for transfer files()
```

Most parameter keys can be obtained from the template as long as the directories and files exist
- Use `cp params.json.template params.json`
- AWS credentials must be obtained legitimately; the indicator will fail without them.

### Setting Dates
- Pull start/end dates refer to the date range of input data obtained from Quidel.
- Export start/end dates refer to csv export dates used in the `create_export_csv` function.
- Latest exported file date will 5 days before the `export_end_date`; if set to null it will be 5 days before the current time.
- It is advisable to keep the length between start and end dates to a minimum to save time (but >= 40 days or `export_day_range` if patching).

## Patching - Specific to Quidel

There are no restrictions to running the indicator with the proper credentials.	Issues batches are 40 days (should be the default for `export_day_range` in params).
  - You will need the AWS credentials for both the Quidel source data and the S3 cache to perform patching.

### Directory
Batches uploaded must look like this:

`[name-of-batch]/issue_[patch-date]/quidel/`
- Ex: `patch_quidel_dec/issue_20221212/quidel/`
- Contains archive diff'd csv files.
- Dates should match with database (40 day length, see below).

### Database
Issue batch date ranges can be viewed in the database with the command:

```
select issue, max(time_value), min(time_value) from epimetric_full_v where source="quidel" and `signal`="covid_ag_raw_pct_positive" and time_type="day" and geo_type="state" and issue>=20230301 and issue<=20230312 group by issue order by issue;
```
The `signal`, `geo_type`, and date ranges can be adjusted.
- check the API for a breakdown of combinations relevant to Quidel signals.

Here is part of results the above command:
```
+----------+-----------------+-----------------+
| issue    | max(time_value) | min(time_value) |
+----------+-----------------+-----------------+
| 20230301 |        20230224 |        20230116 |
| 20230302 |        20230225 |        20230116 |
| 20230303 |        20230226 |        20230117 |
| 20230304 |        20230227 |        20230118 |
| 20230305 |        20230228 |        20230121 |
+----------+-----------------+-----------------+
```

## Archive
Quidel utilizes the S3 archiver in production with indicator prefix `quidel`.
- `params.json` requires `archive` section (see below).
- `common` section needs the `export_dir`, but `log_filename` can and should be used to aid in the case of issues.

### S3 Archive
S3 archive differ should mainly be used by the production environment during a scheduled indicator run (not a manual run) as items in the S3 cache can be overwritten. Here are the archive params:
```
"common": {
    "export_dir": "receiving",
    "log_filename": "dsew_cpr.log"
},
"archive": {
  "aws_credentials: {
	  "aws_access_key_id": "",
    "aws_secret_access_key": ""
	},
	"bucket_name": "delphi-covidcast-indicator-output",
  "cache_dir": "./archivediffer_cache",
  "indicator_prefix": "quidel"
}
```
NOTES: 
- AWS keys need obtained legitimately (redacted here).
- These parameters will be needed to obtain the S3 cache for patching, but running `versions.py` will not overwrite in the case or archiving.

### Filesystem Archive
To use filesystem differ, the only key in `archive` should be the `cache_dir`. 
`export_dir` key is still needed to perform archiving (`log_filename` is optional).

- This is used to cread archive diffed files for patch batch uploads.
- May have other uses (testing/troubleshooting).

# Other Notes to Add
- Validator and its parameters/functions.
- Backfill has new changes that may adjust setting params.
- Common issues

# The Question 
Should this be more organized or split up? I tried to avoid redundancy with the other notes and the API docs, but it always helps me having a "catch-all" document. Markdown looks better than expected, too.

