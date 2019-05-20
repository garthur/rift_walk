# meta_ingest.py

import os
import pathlib
import dotenv
import datetime
import luigi
import psycopg2
import pandas as pd

dotenv.load_dotenv(dotenv.find_dotenv())

def gen_meta_nodelist(filepath):
    nl = pd.read_excel(filepath, sheetname=0, 
                        usecols=["gameid", "side", "position", "champion", 
                                 "result", "k", "d", "a"])
    # get rid of team observations
    nl = nl[nl["position"] != "Team"]
    return nl

def gen_meta_edgelist(filepath):
    import itertools as itr
    # read in the data
    df = pd.read_excel(filepath, sheetname=0,
                       usecols=["gameid", "side", "position", "champion", 
                                "ban1", "ban2", "ban3", "ban4", "ban5"])
    # get rid of team observations
    df = df[df["position"] != "Team"]
    
    # initialize empty data frames
    pw = pd.DataFrame()
    ba = pd.DataFrame()

    def gen_pw(group_df):
        new = pd.DataFrame()
        new["pick_combos"] = list(itr.combinations(group_df.champion, 2))
        
        new[["champ_a", "champ_b"]] = pd.DataFrame(new.pick_combos.values.tolist(), 
                                                        index=new.index)
        new = new.drop(["pick_combos"], axis = 1)
        new["link_type"] = "pick"
        new["gameid"] = group_df.gameid.values[0]
        return new
    
    def gen_ba(group_df):
        new = pd.DataFrame()
        # loop through ban columns
        ban_combos = []
        for ban in range(1,6):
            ban_combos.extend(list(zip(group_df.champion.values, eval("group_df.ban%s.values" % ban))))
        new["ban_combos"] = ban_combos
        new[["champ_a", "champ_b"]] = pd.DataFrame(new.ban_combos.values.tolist(), 
                                                        index=new.index)
        new = new.drop(["ban_combos"], axis = 1)
        new["link_type"] = "ban"
        new["gameid"] = group_df.gameid.values[0]
        return new
    
    # loop through all the groups
    for name, group in df.groupby(["gameid", "side"], as_index=False):
        pw = pw.append(gen_pw(group))
        # grab banned against
        ba = ba.append(gen_ba(group))

    return pw.append(ba)

def gen_meta_info(filepath):
    meta_info = pd.read_excel(filepath, sheetname=0,
                              usecols=["gameid", "league", "split", "date", 
                                       "week", "patchno"])

    return meta_info.drop_duplicates()

class MetaDBSetup(luigi.Task):

    def run(self):
        
        conn = psycopg2.connect(
            host = os.environ.get("AWS_DATABASE_URL"),
            dbname = os.environ.get("AWS_DATABASE_NAME"),
            user = os.environ.get("AWS_DATABASE_USER"),
            password = os.environ.get("AWS_DATABASE_PW")
        )

        cur = conn.cursor()

        # create tables
        
        # metadata
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS meta_info (
                gameid integer PRIMARY KEY,
                league char,
                split char,
                game_date date,
                week char,
                patchno char
            );
            """
        )
        # edgelist
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS meta_edgelist (
                gameid integer REFERENCES meta_info(gameid) ON DELETE CASCADE,
                champ_a char,
                champ_b char,
                side char,
                link_type char(2)
            );
            """
        )
        # nodelist
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS meta_nodelist (
                gameid integer REFERENCES meta_info(gameid) ON DELETE CASCADE,
                side char,
                position char,
                champ char,
                result integer,
                k integer CHECK (k >= 0),
                d integer CHECK (d >= 0),
                a integer CHECK (a >= 0)
            );
            """
        )

        # finish up
        conn.commit()
        conn.close()

class MetaUpload(luigi.Task):
    ingest_path = luigi.Parameter(default="/data/raw")

    def requires(self):
        return [MetaDBSetup()]

    def run(self):

        conn = psycopg2.connect(
            host = os.environ.get("AWS_DATABASE_URL"),
            dbname = os.environ.get("AWS_DATABASE_NAME"),
            user = os.environ.get("AWS_DATABASE_USER"),
            password = os.environ.get("AWS_DATABASE_PW")
        )

        cur = conn.cursor()

        # read file information
        files = []
        for r, d, f in os.walk(ingest_path):
            for f_ in f:
                if ".xlsx" in f_:
                    files.append(os.path.join(r, f_))


        # upload to db
        
        # edgelist
        cur.execute(

        )
        # nodelist
        cur.execute(

        )
        # metadata
        cur.execute(

        )

        # close connection
        conn.close()