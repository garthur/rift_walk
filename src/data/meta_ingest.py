# meta_ingest.py

import os
import pathlib
import urllib.parse
import itertools as itr
import psycopg2
import pandas as pd

# load environment variables
import dotenv
dotenv.load_dotenv(dotenv.find_dotenv())

# spark setup
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, ArrayType, StringType, IntegerType, DateType

# initialize spark context
def __init_spark():
    global sc
    global sqlContext
    try:
        sc and sqlContext
    except NameError as e:
        sc = SparkContext()
        sqlContext = SparkSession(sc)

def read_oracle_data(f):
    __init_spark()

    # read in the data
    df = sqlContext.read.csv(f, header=True)

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
             .dropDuplicates()\
             .withColumn("game_date", F.when(F.col("game_date") != "", F.col("game_date")))\
             .withColumn("game_date", F.from_unixtime(F.unix_timestamp("game_date", "mm/dd/yyyy"), 'yyyy-mm-dd'))\
             .withColumn("game_date", F.col("game_date").cast(DateType()))

    # nodelist frame
    node = df.map(lambda x: (x[0], *x[7:14]))\
             .toDF(["gameid", "side", "position", "champ", "result", "k", "d", "a"])\
             .withColumn("result", F.col("result").cast(IntegerType()))\
             .withColumn("k", F.col("k").cast(IntegerType()))\
             .withColumn("d", F.col("d").cast(IntegerType()))\
             .withColumn("a", F.col("a").cast(IntegerType()))

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

def __prep_dbconnect():
    
    conn = psycopg2.connect(
        host = os.environ.get("AWS_DATABASE_URL"),
        dbname = os.environ.get("AWS_DATABASE_NAME"),
        user = os.environ.get("AWS_DATABASE_USER"),
        password = os.environ.get("AWS_DATABASE_PW")
    )

    cur = conn.cursor()
    
    return conn, cur

def __prep_sparkconnect():
    url = "jdbc:postgresql://%s:%s/%s" \
        % (os.environ.get("AWS_DATABASE_URL"), os.environ.get("AWS_DATABASE_PORT"),
           os.environ.get("AWS_DATABASE_NAME"))
    
    properties = {
        "user":os.environ.get("AWS_DATABASE_USER"),
        "password":os.environ.get("AWS_DATABASE_PW"),
        "driver":"org.postgresql.Driver"  
    }

    return url, properties

def meta_db_clean():

    conn, cur = __prep_dbconnect()

    # drop tables

    ## metadata
    cur.execute(
        """
        DROP TABLE IF EXISTS meta_info CASCADE;
        """
    )
    ## edgelist
    cur.execute(
        """
        DROP TABLE IF EXISTS meta_edgelist CASCADE;
        """
    )
    ## nodelist
    cur.execute(
        """
        DROP TABLE IF EXISTS meta_nodelist CASCADE;
        """
    )

    # finish up
    conn.commit()
    conn.close()

def meta_db_setup():
    
    conn, cur = __prep_dbconnect()

    # create tables
    
    ## metadata
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
    ## edgelist
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
    ## nodelist
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS meta_nodelist (
            gameid TEXT REFERENCES meta_info(gameid) ON DELETE CASCADE,
            side TEXT,
            position TEXT,
            champ TEXT,
            result INTEGER,
            k INTEGER,
            d INTEGER,
            a INTEGER
        );
        """
    )

    # finish up
    conn.commit()
    conn.close()

def push_oracle_data(ingest_dir):
    __init_spark()

    # read file information
    files = []
    for r, d, f in os.walk(ingest_dir):
        for f_ in f:
            if ".csv" in f_:
                files.append(os.path.join(r, f_))

    # prep connection    
    url, properties = __prep_sparkconnect()

    for f in files:
        print(f, "READING...")
        # read in data
        info, node, edge = read_oracle_data(f)
        print(f, "UPLOADING...")
        # write up to postgresql
        info.write.jdbc(url=url, table="meta_info", mode="append", properties=properties)
        node.write.jdbc(url=url, table="meta_nodelist", mode="append", properties=properties)
        edge.write.jdbc(url=url, table="meta_edgelist", mode="append", properties=properties)
        # clear frames
        del info, node, edge
        print(f, "DONE!")

def fetch_oracle_data(subset):
    __init_spark()
    # prep connection
    url, properties = __prep_sparkconnect()

    #TODO: fix this dynamic query
    _info_sql = """
        (SELECT * FROM meta_info WHERE
            ('{0}' = 'None' or league = '{0}') AND
            ('{1}' = 'None' or split = '{1}') AND
            ('{2}' = 'None' or '{3}' = 'None' or game_date between '{2}' and '{3}') AND
            ('{4}' = 'None' or patchno = '{4}')
        ) AS info;
    """
    _node_sql = """
        (SELECT * from meta_nodelist WHERE
            gameid in {}
        ) AS node;
    """
    _edge_sql = """
        (SELECT * from meta_edgelist WHERE
            gameid in {}
        ) AS edge;
    """

    print(_info_sql.format(subset["league"], subset["split"], 
                            subset["start_date"], subset["end_date"], 
                            subset["patchno"])

    info = sqlContext.read.jdbc(url=url, 
                                table=_info_sql.format(
                                    subset["league"], subset["split"], 
                                    subset["start_date"], subset["end_date"], 
                                    subset["patchno"]
                                ), 
                                properties=properties)

    gameid_arr = str([str(row.gameid) for row in info.select("gameid").collect()])
    gameid_arr = gameid_arr.replace("[", "{")\
                           .replace("]", "}")\
                           .replace("\n", ",")

    node = sqlContext.read.jdbc(url=url,
                                table = _node_sql.format(gameid_arr),
                                properties=properties)
    
    edge = sqlContext.read.jdbc(url=url,
                                table=_edge_sql.format(gameid_arr),
                                properties=properties)

    return info, node, edge