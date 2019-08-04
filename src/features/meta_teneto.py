import itertools as itr
import pandas as pd
import numpy as np
import teneto
import h5py
import json
import uuid
import datetime

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

class TTeneto(teneto.classes.TemporalNetwork):
    """
    Extends the teneto `TemporalNetwork` class with utility methods 
    for this analysis. Most functionality remains the same as with 
    a regular `TemporalNetwork`.
    """

    def __init__(self, info, edge, link_type="pkw", weighted=False):
        """
        Create a Teneto Network from an info frame, an edge frame,
        and a node frame.
        """

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
        
        super().__init__(from_df=full_dat, nodelabels=list(champ_key.keys()))

    @classmethod
    def load(f):
        """
        Load a TTeneto object from an HDF5 file.

        Arguments:

        f -- an HDF5 file dumped by a TTeneto object.
        """
        pass

    def dump(self, f_):
        """
        Dump a TTeneto object to an HDF5 file.

        Arguments:

        f_ -- An HDF5 file to be dumped to. Does not have to exist.
        """
        # TODO: finish this
        with h5py.File(f_, "w") as f:
            
            network = f.create_dataset("network")

            # graph parameters

            # file metadata
            info = f.create_group("metadata")
            info.attrs["uid"] = str(uuid.uuid4())
            info.attrs["creation_date"] = str(datetime.datetime.now())

            f.close()

    