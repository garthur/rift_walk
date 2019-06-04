"""Rift Walk - lol-meta-analysis

Usage:
    rift-walk-cli ingest <oracle-dir> --overwrite-db
    rift-walk-cli pull <output-dir> [(--league <league>) (--split <split>) (--start <start>) (--end <end>) (--patch <patch>)]
    rift-walk-cli -h

Arguments:
    <oracle-dir>                        Directory with csv files from 'https://oracleselixir.com/match-data/'.
    <output-dir>                        Directory to write parquet files to. Files will be saved in a subdirectory.

Options:
    -h --help                           Show this screen.

"""
from docopt import docopt

def main():
    arguments = docopt(__doc__)
    
    if arguments["ingest"]:
        from src.data import meta_ingest
        
        if(arguments["--overwrite-db"]):
            meta_ingest.meta_db_clean()
            meta_ingest.meta_db_setup()
        
        meta_ingest.push_oracle_data(arguments["<oracle-dir>"])
    
    elif arguments["pull"]:
        from src.data import meta_ingest

        subset = {}
        subset["league"] = arguments["<league>"] if arguments["--league"] else None
        subset["split"] = arguments["<split>"] if arguments["--split"] else None
        subset["start_date"] = arguments["<start>"] if arguments["--start"] else "2000-01-01"
        subset["end_date"] = arguments["<end>"] if arguments["--end"] else "2050-01-01"
        subset["patchno"] = arguments["<patch>"] if arguments["--patch"] else None

        meta_ingest.pull_oracle_data(arguments["<output-dir>"], subset)

if __name__ == "__main__":
    main()