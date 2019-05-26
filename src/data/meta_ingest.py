# meta_ingest.py

import os
import pathlib
import urllib.parse
import luigi
import psycopg2
import pandas as pd

# load environment variables
import dotenv
dotenv.load_dotenv(dotenv.find_dotenv())

# spark setup
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
CONF = SparkConf().setAppName("meta_ingest").setMaster("local")
SC = SparkContext(conf=CONF)
SQL_CONTEXT = SparkSession(SC)

# constants
MAJOR_LEAGUES = ["NALCS", "LCS", "EUCLS", "LEC", "LPL", "LMS", "LCK", "WC", "MSI"]

def __make_gameid(r):
    if r.isnull()[1]:
        gameid = r[0]
    else:
        parsed = urllib.parse.parse_qs(
            urllib.parse.urlparse(r[1], allow_fragments=False).query
        )

        if "gameHash" in parsed:
            gameid = str(parsed["gameHash"][0])
        else:
            gameid = r[0]
    
    r_ = [gameid, gameid]
    r_.extend(r[2:])
    
    return r_

def __validate_gameid(gameid, url):
    parsed = urllib.parse.parse_qs(
                urllib.parse.urlparse(url, allow_fragments=False).query
             )
    if "gameHash" in parsed:
        return str(parsed["gameHash"][0])
    else:
        return gameid

def __update_leagues(l):
    l_dict = {"NALCS":"LCS", "EULCS":"LEC", "LPL":"LPL", 
              "LMS":"LMS", "LCK":"LCK", "WC":"WC", "MSI":"MSI"}
    
    return l_dict[l]

def gen_meta_info(f):
    meta_info = pd.read_excel(f, sheet_name=0,
                                usecols=["gameid", "url", "league", "split", "date", 
                                        "week", "patchno"],
                                dtype={"gameid":str, "url":str, "league":str, "split":str,
                                        "week":str, "patchno":str})

    meta_info = meta_info.apply(__make_gameid, axis=1, result_type="broadcast")
    # keep only major league games
    meta_info = meta_info[meta_info["league"].isin(MAJOR_LEAGUES)]
    # update league names
    meta_info.league = meta_info.league.apply(__update_leagues)
    # drop unecessary columns
    meta_info = meta_info.drop(["url"], axis=1)
    
    # drop duplicates
    return meta_info.drop_duplicates()

def gen_meta_nodelist(f, meta_info):
    nl = pd.read_excel(f, sheet_name=0, 
                        usecols=["gameid", "url", "side", "position", "champion", 
                                 "result", "k", "d", "a"],
                        dtype={"gameid":str, "url":str, "side":str, "position":str, 
                                "champion":str, "result":int, "k":int, "d":int, "a":int})
    # get rid of team observations
    nl = nl[nl["position"] != "Team"]
    # make and select valid gameid
    nl = nl.apply(__make_gameid, axis=1, result_type="broadcast")
    nl = nl[nl["gameid"].isin(meta_info.gameid.values)]
    # drop unecessary columns
    nl = nl.drop(["url"], axis=1)
    return nl

def gen_meta_edgelist(f, meta_info):
    import itertools as itr
    # read in the data
    df = pd.read_excel(f, sheet_name=0,
                       usecols=["gameid", "url", "side", "position", "champion", 
                                "ban1", "ban2", "ban3", "ban4", "ban5"],
                       dtype={"gameid":str, "url":str, "side":str, "position":str, "champion":str,
                              "ban1":str, "ban2":str, "ban3":str, "ban4":str, "ban5":str})
    # get rid of team observations
    df = df[df["position"] != "Team"]
    # make and select valid gameid
    df = df.apply(__make_gameid, axis=1, result_type="broadcast")
    df = df[df["gameid"].isin(meta_info.gameid.values)]
    # drop unecessary columns
    df = df.drop(["url"], axis=1)
    
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

def read_oracle_data(f):
    
    # read in the data
    df = SQL_CONTEXT.read.csv(f, header=True)
    
    # drop unnecessary columns
    df = df.select("gameid", "url", "league", "split", "date", "week", "patchno",
                   "side", "position", "champion", "result", "k", "d", "a",
                   "ban1", "ban2", "ban3", "ban4", "ban5").rdd
    # filter rows
    df = df.filter(lambda x: (x[2] in MAJOR_LEAGUES) and (x[8] != "Team"))
    # update league and gameid
    df = df.map(lambda x: (*x[0:2], __update_leagues(x[2]), *x[3:]))\
           .map(lambda x: (__validate_gameid(x[0], x[1]), *x[1:]))

    # metadata frame
    info = df.map(lambda x: (x[0], *x[2:7]))\
             .toDF(["gameid", "league", "split", "game_date", "week", "patchno"])\
             .dropDuplicates()

    # nodelist frame
    node = df.map(lambda x: (x[0], *x[7:14]))\
             .toDF("gameid", "side", "position", "champion", "result", "k", "d", "a")

    # edgelist frame
    ban = df.map(lambda x: (x[0], (x[9], x[14]), (x[9], x[15]), (x[9], x[16]),
                            (x[9], x[17]), (x[9], x[18]), "ban" ))
    
    return info, node, None
    

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
                game_date TEXT,
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

        # read file information
        files = []
        for r, d, f in os.walk(self.ingest_dir):
            for f_ in f:
                if ".xlsx" in f_:
                    files.append(os.path.join(r, f_))

        # insert sql        
        insert_info = """
                      INSERT INTO meta_info (gameid, league, split, game_date, week, patchno)
                      VALUES (%s, %s, %s, %s, %s, %s);
                      """
        insert_el = """
                    INSERT INTO meta_edgelist (champ_a, champ_b, link_type, gameid) 
                    VALUES (%s, %s, %s, %s);
                    """
        insert_nl = """
                    INSERT INTO meta_nodelist (gameid, side, position, champ, result, k, d, a)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """

        # upload to db
        for f in files:
            
            # connect to db
            conn = psycopg2.connect(
                host = os.environ.get("AWS_DATABASE_URL"),
                dbname = os.environ.get("AWS_DATABASE_NAME"),
                user = os.environ.get("AWS_DATABASE_USER"),
                password = os.environ.get("AWS_DATABASE_PW"),
                connect_timeout = 18000
            )
            cur = conn.cursor()

            print(f, "UPLOADING...")

            # metadata
            info = gen_meta_info(f)
            cur.executemany(
                query = insert_info,
                vars_list = [tuple(row)[1:] for row in info.itertuples()]
            )
            # edgelist
            el = gen_meta_edgelist(f, info)
            cur.executemany(
                query = insert_el,
                vars_list = [tuple(row)[1:] for row in el.itertuples()]
            )
            del el
            # nodelist
            nl = gen_meta_nodelist(f, info)
            cur.executemany(
                query = insert_nl,
                vars_list = [tuple(row)[1:] for row in nl.itertuples()]
            )
            del nl, info

            print(f, "DONE!!!")

        # close connection
        conn.commit()
        conn.close()

        self.__complete = True

    def complete(self):
        return self.__complete