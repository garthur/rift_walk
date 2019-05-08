# meta_ingest.py

import pandas as pd

def gen_meta_nodelist(filepath):
    nl = pd.read_excel(filepath, sheetname=0, 
                        usecols=["gameid", "side", "position", "champion", 
                                 "result", "k", "d", "a"])
    pass

def gen_meta_edgelist_ud(filepath):
    pass

def gen_meta_info(filepath):
    meta_info = pd.read_excel(filepath, sheetname=0,
                              usecols=["gameid", "league", "split", "date", 
                                       "week", "patchno"])

    return meta_info.drop_duplicates()