import itertools as itr

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

class TGraphFrame(object):
    """
    A Temporal GraphFrame is a time-ordered list of Spark GraphFrames
    representing a Temporal Network. A variety of methods are available,
    including temporal network measures from `teneto`.
    """

    def __init__(self, info, node, edge, link="pkw"):
        """
        Create a Temporal GraphFrame from an info frame, an edge frame,
        and a node frame.
        """
        if(link == "ban"):
            raise ValueError("Temporal GraphFrames do not yet support ban-linked networks.")

        self.link = link

        min_date = str(info.select(F.min("game_date")).first()[0])

        self.metadata = info.withColumn("t", F.expr("DATEDIFF(game_date, '{}')".format(min_date)))\
                            .withColumn("t", F.expr("DENSE_RANK() OVER (ORDER BY t)"))\
                            .sort("t", ascending=True)

        game_order = [[row.gameid for row in self.metadata.filter(self.metadata.t == t_).collect()] 
                        for t_ in self.metadata.select("t").distinct().rdd.flatMap(lambda x: x).collect()]
        
        # edgelist
        ## only get relevant links
        edge = edge.filter(edge.link_type == link)
        ## TODO: collapse edgelist
        
        ## generate a list of edge frames splitting on game order
        t_edge = [edge.filter(edge.gameid in games)\
                        .selectExpr("champ_a as src", "champ_b as dst")\
                        .withColumn("games", games) for games in game_order]

        ## TODO: test nodelist generation
        
        ## generate a list of node frames splitting on game order
        t_node = [node.filter(lambda x: x[0] in games)\
                      .groupBy("champ")\
                      .agg(F.collect_list("side").alias("side"),
                           F.collect_list("position").alias("position"),
                           F.collect_list("result").alias("result"),
                           F.collect_list("k").alias("k"),
                           F.collect_list("d").alias("d"),
                           F.collect_list("a").alias("a"))\
                      .select("champ as id", "*")\
                      .withColumn("games", games) for games in game_order]

        self.games = game_order

    @staticmethod
    def load(f):
        """
        Load a TGraphFrame from an HDF5 file.

        Arguments:

        f -- An HDF5 file dumped by a TGraphFrame object.
        """
        pass
    
    def dump(self, f):
        """
        Dump a Temporal GraphFrame to an HDF5 file.

        Arguments:

        f -- An HDF5 file to be dumped to. Does not have to exist.
        """
        pass

    def split(self, fct_variable):
        """
        Split the temporal network into one or more by a factor variable
        given in the `info` data frame.

        Arguments:

        fct_variable -- A factor variable which the network will be split into.
        """
        pass




