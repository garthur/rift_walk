# meta_ingest.py

import os
import re
import pathlib
import urllib.parse
import dotenv
import datetime
import luigi
import psycopg2
import pandas as pd

dotenv.load_dotenv(dotenv.find_dotenv())

def gen_meta_nodelist(filepath):
    nl = pd.read_excel(filepath, sheet_name=0, 
                        usecols=["url", "side", "position", "champion", 
                                 "result", "k", "d", "a"])
    # get rid of team observations
    nl = nl[nl["position"] != "Team"]
    # no valid gameid
    nl = nl[nl.url.notnull()]
    nl["gameid"] = nl.url.apply(
        lambda x: str(urllib.parse.parse_qs(
            urllib.parse.urlparse(x, allow_fragments=False).query
        )["gameHash"][0])
    )
    nl = nl.drop(["url"], axis=1)
    return nl

def gen_meta_edgelist(filepath):
    import itertools as itr
    # read in the data
    df = pd.read_excel(filepath, sheet_name=0,
                       usecols=["url", "side", "position", "champion", 
                                "ban1", "ban2", "ban3", "ban4", "ban5"])
    # get rid of team observations
    df = df[df["position"] != "Team"]
    # no valid gameid
    df = df[df.url.notnull()]
    df["gameid"] = df.url.apply(
        lambda x: str(urllib.parse.parse_qs(
            urllib.parse.urlparse(x, allow_fragments=False).query
        )["gameHash"][0])
    )
    
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
    meta_info = pd.read_excel(filepath, sheet_name=0,
                              usecols=["url", "league", "split", "date", 
                                       "week", "patchno"])

    # no valid gameid
    meta_info = meta_info[meta_info.url.notnull()]
    meta_info["gameid"] = meta_info.url.apply(
        lambda x: str(urllib.parse.parse_qs(
            urllib.parse.urlparse(x, allow_fragments=False).query
        )["gameHash"][0])
    )
    meta_info = meta_info.drop(["url"], axis=1)
    return meta_info.drop_duplicates()

class MetaDBClean(luigi.Task):
    __complete = False

    def run(self):

        conn = psycopg2.connect(
            host = os.environ.get("AWS_DATABASE_URL"),
            dbname = os.environ.get("AWS_DATABASE_NAME"),
            user = os.environ.get("AWS_DATABASE_USER"),
            password = os.environ.get("AWS_DATABASE_PW")
        )

        cur = conn.cursor()

        # drop tables

        # metadata
        cur.execute(
            """
            DROP TABLE IF EXISTS meta_info CASCADE;
            """
        )
        # edgelist
        cur.execute(
            """
            DROP TABLE IF EXISTS meta_edgelist CASCADE;
            """
        )
        # nodelist
        cur.execute(
            """
            DROP TABLE IF EXISTS meta_nodelist CASCADE;
            """
        )

        # finish up
        conn.commit()
        conn.close()

        self.__complete = True
    
    def complete(self):
        return self.__complete

class MetaDBSetup(luigi.Task):
    overwrite = luigi.BoolParameter(default=False)
    __complete = False

    def requires(self):
        if self.overwrite:
            return MetaDBClean()

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
                gameid TEXT PRIMARY KEY,
                league TEXT,
                split TEXT,
                game_date DATE,
                week TEXT,
                patchno TEXT
            );
            """
        )
        # edgelist
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS meta_edgelist (
                gameid TEXT REFERENCES meta_info(gameid) ON DELETE CASCADE,
                champ_a TEXT,
                champ_b TEXT,
                link_type TEXT
            );
            """
        )
        # nodelist
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS meta_nodelist (
                gameid TEXT REFERENCES meta_info(gameid) ON DELETE CASCADE,
                side TEXT,
                position TEXT,
                champ TEXT,
                result INTEGER,
                k INTEGER CHECK (k >= 0),
                d INTEGER CHECK (d >= 0),
                a INTEGER CHECK (a >= 0)
            );
            """
        )

        # finish up
        conn.commit()
        conn.close()

        self.__complete = True

    def complete(self):
        return self.__complete

class MetaUpload(luigi.Task):
    overwrite = luigi.BoolParameter(default=False)
    ingest_dir = luigi.Parameter(default="/data/raw")
    __complete = False

    def requires(self):
        return MetaDBSetup(self.overwrite)

    def run(self):

        conn = psycopg2.connect(
            host = os.environ.get("AWS_DATABASE_URL"),
            dbname = os.environ.get("AWS_DATABASE_NAME"),
            user = os.environ.get("AWS_DATABASE_USER"),
            password = os.environ.get("AWS_DATABASE_PW"),
            connect_timeout = 18000
        )

        cur = conn.cursor()

        # read file information
        files = []
        for r, d, f in os.walk(self.ingest_dir):
            for f_ in f:
                if ".xlsx" in f_:
                    files.append(os.path.join(r, f_))

        # insert sql
        
        
        insert_info = """
                      INSERT INTO meta_info (league, split, game_date, week, patchno, gameid)
                      VALUES (%s, %s, %s, %s, %s, %s);
                      """
        insert_el = """
                    INSERT INTO meta_edgelist (champ_a, champ_b, link_type, gameid) 
                    VALUES (%s, %s, %s, %s);
                    """
        insert_nl = """
                    INSERT INTO meta_nodelist (side, position, champ, result, k, d, a, gameid)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """

        # upload to db
        for f in files:

            # metadata
            info = gen_meta_info(f)
            cur.executemany(
                query = insert_info,
                vars_list = [tuple(row)[1:] for row in info.itertuples()]
            )
            del info
            # edgelist
            el = gen_meta_edgelist(f)
            cur.executemany(
                query = insert_el,
                vars_list = [tuple(row)[1:] for row in el.itertuples()]
            )
            del el
            # nodelist
            nl = gen_meta_nodelist(f)
            cur.executemany(
                query = insert_nl,
                vars_list = [tuple(row)[1:] for row in nl.itertuples()]
            )
            del nl

        # close connection
        conn.commit()
        conn.close()

        self.__complete = True

    def complete(self):
        return self.__complete