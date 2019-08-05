"""Rift Walk - lol-meta-analysis

Usage:
    rift-walk-cli ingest <input-dir> --overwrite-db
    rift-walk-cli pull <output-dir> [(--league <league>) (--split <split>) (--start <start>) (--end <end>) (--patch <patch>)]
    rift-walk-cli ntwk <output-file> <ntwk-driver> (--pkw|--pka|--ban) [(--league <league>) (--split <split>) (--start <start>) (--end <end>) (--patch <patch>)]
    rift-walk-cli -h

Arguments:
    <input-dir>                        Directory with csv files from 'https://oracleselixir.com/match-data/'.
    <output-dir>                       Directory to write parquet files to. Files will be saved in a subdirectory.
    <output-file>                      H5 file to write to. Will be created if it does not exist.
    <ntwk-driver>                      Network Driver to be used. One of g, t, n.

Options:
    -h --help                           Show this screen.

"""
from docopt import docopt
from src.data import meta_ingest
from src.features import TNetworkX

def main():
    arguments = docopt(__doc__)
    
    if arguments["ingest"]:
        
        if(arguments["--overwrite-db"]):
            meta_ingest.meta_db_clean()
            meta_ingest.meta_db_setup()
        
        # push a directory to db
        meta_ingest.push_oracle_data(arguments["<input-dir>"])
    
    elif arguments["pull"]:

        # construct subset dict
        subset = {}
        subset["league"] = arguments["<league>"] if arguments["--league"] else None
        subset["split"] = arguments["<split>"] if arguments["--split"] else None
        subset["start_date"] = arguments["<start>"] if arguments["--start"] else "2000-01-01"
        subset["end_date"] = arguments["<end>"] if arguments["--end"] else "2050-01-01"
        subset["patchno"] = arguments["<patch>"] if arguments["--patch"] else None

        # pull down data and save in output-dir
        meta_ingest.pull_oracle_data(arguments["<output-dir>"], subset)

    elif arguments["ntwk"]:

        # construct subset dict
        subset = {}
        subset["league"] = arguments["<league>"] if arguments["--league"] else None
        subset["split"] = arguments["<split>"] if arguments["--split"] else None
        subset["start_date"] = arguments["<start>"] if arguments["--start"] else "2000-01-01"
        subset["end_date"] = arguments["<end>"] if arguments["--end"] else "2050-01-01"
        subset["patchno"] = arguments["<patch>"] if arguments["--patch"] else None

        # fetch oracle data
        i, n, e = meta_ingest.fetch_oracle_data(subset)

        if arguments["<driver>"] == "g":
            raise ValueError("TGraphFrames are not yet supported.")
        elif arguments["<driver>"] == "t":
            raise ValueError("TTeneto Networks are not yet supported.")
        elif arguments["<driver>"] == "n":
            N = TNetworkX(i, n, e, gtype="simple")
        else:
            raise ValueError("An incorrect driver was specified.")

            

if __name__ == "__main__":
    main()