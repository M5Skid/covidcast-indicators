# Quidel Covidtest Notes
This is a working document designed to compile information into a user friendly FAQ style guide

## Intro
Quidel sources information based on covid antigen test data. Signal data consists of percentages of antigen tests that were positive for covid.

### Signals
- Ex: `covid_ag_raw_pct_positive_age_18_49`
- Split up by various age groups
- Both a smoothed and raw signal are created for each group

### Data Collection
- Python module connects to AWS 
- Requires AWS access key and ID (Separate from archive differ)

## Running the Indicator
The module name and venv/makefile information should be in the quidel_covidtest directory. Provided the params are specified, the indicator can be run in a local environment.

### Directory Structure
The "quidel_covidtest" indicator directory is expected to have the following subdirectories and files. Many can be overwritten in params.json (see below)

Run Module
- `delphi_quidel_covidtest/` - contains indicator code

Functional Directories/Files
- `backfill/` - backfill dir for parquet files
- `cache/` - input cache for pulled data
- `receiving/` - contains .csv export, also delivery dir for acquisition
- `logs/quidel-covidtest.log` - module log printed by structured logger. Will contain additional logging if delphi_utils functions are run on the indicator (like validator)
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

Most param keys can be obtained from the template as long as the directories and files exist
-  Aws creds must be obtained legitimately

Setting dates:
- pull start/end dates refer to the date range of input data obtained from quidel
- export start/end dates refer to csv export dates used in the create_export_csv_function
- Latest exported file date will 5 days before the export_end_date; if set to null it will be 5 days before the current time
- It is advisable to keep the length between start and end dates to a minimum to save time (but >= 40 days or export_day_range if patching)

## Patching - Specific to Quidel

There are no restrictions to running the indicator with the proper credentials.	Issues batches are 40 days (should be the default for export_day_range in params).

### Directory
[name-of-batch]/issue_[patch-date]/quidel
- contains archive diff'd csv files
- dates should match with database (40 day length, see below)

### Database
Issue batch date ranges can be viewed in the database with the command:

```
select issue, max(time_value), min(time_value) from epimetric_full_v where source="quidel" and `signal`="covid_ag_raw_pct_positive" and time_type="day" and geo_type="state" and issue>=20230301 and issue<=20230312 group by issue order by issue;
```
The signal, time_type, geo_type, and dates can be adjusted.
- check the API for a breakdown of combinations

Here is part of the above command:
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
Quidel utilizes the s3 archive in production with indicator prefix quidel
- Archive requires parameters section
- common section needs the export dir

### S3 Archive
S3 archive differ should mainly be used by the production environment during a scheduled indicator run (not a manual run). Here are the params:
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
NOTE: AWS keys need obtained from staging.
The `common` section must also include `export_dir` (`log_filename` is always recommended)

### Filesystem Archive
To use filesystem differ, the only key in `archive` should be the cache_dir. `export_dir` key is still needed to perform archiving.

# Other Notes
- validator
- Backfill has new changes that may adjust setting params.
- common issues
	

