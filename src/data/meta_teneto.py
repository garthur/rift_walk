import itertools as itr
import pandas as pd
import teneto

# load environment variables
import dotenv
dotenv.load_dotenv(dotenv.find_dotenv())

# spark setup
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# initialize spark context
def __init_spark():
    global sc
    global sqlContext
    try:
        sc and sqlContext
    except NameError as e:
        sc = SparkContext()
        sqlContext = SparkSession(sc)

def make_teneto_graph(info, edge, link_type="pkw", weighted=False):
    

    # join frames together
    full_dat = info.join(edge, info.gameid == edge.gameid, how="right")

    # get date difference
    min_date = str(full_dat.select(F.min("game_date")).first()[0])

    full_dat = full_dat.withColumn("t", F.expr("DATEDIFF(game_date, '{}')".format(min_date)))\
                       .filter(full_dat.link_type == link_type)\
                       .select("champ_a", "champ_b", "t")\
                       .withColumn("t", F.expr("DENSE_RANK() OVER (ORDER BY t)"))\
                       .sort("t", ascending=True)
    
    if weighted: 
        pass # TODO: implement weighted graphs
    else:
        full_dat = full_dat.dropDuplicates()

    # serialize champ nodes
    champ_list = [i[0] for i in full_dat.select("champ_a").alias("champ")\
                                .union(full_dat.select("champ_b").alias("champ"))\
                                .distinct().collect()]
    champ_key = {name:idx for idx, name in enumerate(champ_list)}
    champ_map = F.create_map([F.lit(x) for x in itr.chain(*champ_key.items())])

    full_dat = full_dat.select(champ_map[full_dat.champ_a].alias("i"), 
                               champ_map[full_dat.champ_b].alias("j"), 
                               "*")\
                       .drop("champ_a", "champ_b")\
                       .toPandas()

    t_ntwk = teneto.TemporalNetwork(from_df=full_dat, nodelabels=list(champ_key.keys()))

    return t_ntwk
    