# CLI Commands:
## Scraping
To Run:
`python main.py -r <run_number> <optional flags>`

The "-r" or "--run-no" flag is required (they are aliases). It tells the program which "run number" you are working on.
Thus `python main.py -r 2` is equivalent to `python main.py --run-no 2`. It will yell at you if it is not specified.

Other useful flags are:
--output-fn: changes the output filename (default `full_trans`). DO NOT ADD AN EXTENSION. 
e.g. `python main.py -r 2 --output-fn my_special_name` will create a file named `my_special_name.tsv`. 
If you want to modify the extension here, let me know, it's an easy change

--occur-log-interval (must be an integer, default 10): Defines the logging behavior while making requests for occurrences. Default is to output every 10 requests
e.g. `python main.py -r 2 --occur-log-interval 100` will only output every 100 requests

--test-dl: Runs the whole shebang on a limited selection of collections. Good for making sure things work.
e.g. `python main.py -r test_1 --test-dl` Downloads and transforms ~1k Records into `scrape-py/downloads/run_test_1/*`

--no-download: Tells the script to not worry about downloading new information. The script is pretty good at knowing whether or not it needs to download more, but it skips those checks.

--no-transform: Tells the script to not worry about cleaning, deduping, or transforming the raw JSON into TSVs

--no-dedup: Within the transform step, tells the script to scrip deduplication in the "transform" part of the deal.

## Recents:

To run:

`python tsv.py --recent -r <run_number> --days <how many days to go back>`

Args:
--recent: This is required here. I'm planning on using this file to also store the script which will find diffs between different tsvs

-r / --run-no: Required. This will tell the script which directory to look into for the raw data.

The script finds all `tsv`s in `downloads/run_<run_no>/tsvs` and creates subsets of each of those tsvs containing records from the last `x` days. Those files will be named `downloads/run_<run_no>/tsvs/<old_tsv_name>_last<x>days.tsv`.
When re-running the script it will ignore tsv files whose names end with `days.tsv`. 

--days: Optional. Must be int. Default: 30. How many days back to display data.


## API Utils
- `python api.py --total` -- Will hit the API and tell you the total number of occurrences
- `python api.py --coll <int>` -- Will give you the code, name, and number of occurrences held by a the collection designated by that id.
- `python api.py --all-coll` -- Prints all collections, their Codes and Ids
- `python api.py --coll_count` -- Prints current count of collections