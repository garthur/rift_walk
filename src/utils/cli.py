"""Rift Walk - lol-meta-analysis

Usage:
    rift-walk-cli ingest <oracle-dir> --overwrite-db
    rift-walk-cli -h

Arguments:
    <oracle-dir>                        Directory with csv files from 'https://oracleselixir.com/match-data/'.

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
        
        meta_ingest.upload_oracle_dir(arguments["<oracle-dir>"])

if __name__ == "__main__":
    main()