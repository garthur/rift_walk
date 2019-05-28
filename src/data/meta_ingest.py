# meta_ingest.py

import os
import pathlib
import urllib.parse
import itertools as itr
import luigi
import psycopg2
import pandas as pd

# load environment variables
import dotenv
dotenv.load_dotenv(dotenv.find_dotenv())

# spark setup
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

CONF = SparkConf().setAppName("meta_ingest").setMaster("local")
SC = SparkContext(conf=CONF)
SQL_CONTEXT = SparkSession(SC)

def read_oracle_data(f):

    # read in the data
    df = SQL_CONTEXT.read.csv(f, header=True)

    # drop unnecessary columns
    df = df.select("gameid", "url", "league", "split", "date", "week", "patchno",
                    "side", "position", "champion", "result", "k", "d", "a",
                    "ban1", "ban2", "ban3", "ban4", "ban5").rdd
    # filter rows
    MAJOR_LEAGUES = ["NALCS", "LCS", "EUCLS", "LEC", "LPL", "LMS", "LCK", "WC", "MSI"]
    df = df.filter(lambda x: (x[2] in MAJOR_LEAGUES) and (x[8] != "Team"))
    
    # update league and gameid
    def __make_gameid(gameid, url):
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

    df = df.map(lambda x: (*x[0:2], __update_leagues(x[2]), *x[3:]))\
           .map(lambda x: (__make_gameid(x[0], x[1]), *x[1:]))

    # metadata frame
    info = df.map(lambda x: (x[0], *x[2:7]))\
             .toDF(["gameid", "league", "split", "game_date", "week", "patchno"])\
             .dropDuplicates()

    # nodelist frame
    node = df.map(lambda x: (x[0], *x[7:14]))\
             .toDF(["gameid", "side", "position", "champion", "result", "k", "d", "a"])

    # edgelist frame

    ## get bans
    bans = df.map(lambda x: (x[0], [[x[9], x[14]], [x[9], x[15]], [x[9], x[16]],
                            [x[9], x[17]], [x[9], x[18]]], "ban" ))\
             .toDF(["gameid", "combo", "link_type"])\
             .select("gameid", F.explode("combo").alias("combo"), "link_type")
    
    bans = bans.select("gameid",
                       bans.combo.getItem(0).alias("champ_a"),
                       bans.combo.getItem(1).alias("champ_b"),
                       "link_type")
    
    ## get picks and picked against
    def __make_champ_combos(champs, sides):
        cs = list(zip(champs, sides))
        cs = list(itr.combinations(cs, 2))
        return [(p[0][0], p[1][0], "pkw" if p[0][1] == p[1][1] else "pka") for p in cs]

    champ_combo = F.udf(__make_champ_combos, ArrayType(ArrayType(StringType())))

    picks = df.map(lambda x: (x[0], x[7], x[9]))\
              .toDF(["gameid", "side", "champion"])\
              .groupBy("gameid")\
              .agg(F.collect_list("champion").alias("champ"), 
                   F.collect_list("side").alias("side"))\
              .select("gameid", F.explode(champ_combo(F.col("champ"), F.col("side"))).alias("combo"))

    picks = picks.select("gameid", 
                         picks.combo.getItem(0).alias("champ_a"),
                         picks.combo.getItem(1).alias("champ_b"),
                         picks.combo.getItem(2).alias("link_type"))

    ## append picks and bans
    edge = picks.union(bans)

    # return all three frames
    return info, node, edge

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