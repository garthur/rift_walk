import pandas as pd
import numpy as np
import networkx as nx
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

class TNetworkX(object):
    """
    A Temporal GraphFrame is a time-ordered list of NetworkX networks
    representing a Temporal Network. A variety of methods are available,
    including temporal network measures from `teneto`.
    """

    def __init__(self, info, node, edge, link="pkw", gtype="multi"):

        # date order the game information
        min_date = str(info.agg({"game_date":"min"}).collect()[0][0])
        info = info.withColumn("t", F.expr("DATEDIFF(game_date, '{}')".format(min_date)))\
                   .withColumn("t", F.expr("DENSE_RANK() OVER (ORDER BY t)"))\
                   .sort("t", ascending=True)

        # set up network list
        self.periods = info.agg({"t":"max"}).collect()[0][0]
        self.network = [] * self.periods

        # mainloop to input network information
        for t in range(1, self.periods + 1):

            # metadata
            i = info.filter(info.t == t).drop("t")

            # edges
            e = edge.join(i, "gameid", "leftsemi")\
                    .filter(edge.link_type == link)\
                    .select("champ_a", "champ_b", "gameid")\
                    .toPandas()

            # nodes
            n = node.join(i, "gameid", "leftsemi")\
                    .groupBy("champ")\
                    .agg(F.collect_list("side").alias("side"),
                         F.collect_list("position").alias("position"),
                         F.collect_list("result").alias("result"),
                         F.collect_list("k").alias("k"),
                         F.collect_list("d").alias("d"),
                         F.collect_list("a").alias("a"),
                         F.collect_list("gameid").alias("games"))\
                    .toPandas()

            # build the network
            G = nx.from_pandas_edgelist(e, source="champ_a", target="champ_b", edge_attr="gameid", 
                                        create_using=nx.MultiGraph(metadata=i.toPandas()))

            if gtype == "multi":
                # do nothing, we already have a multigraph
                pass 
            elif gtype == "simple":
                # convert to simple graph with weights
                G_ = nx.Graph()
                for u, v, data in G.edges(data=True):
                    w = data["weight"] if "weight" in data else 1.0
                    if G_.has_edge(u, v):
                        G_[u][v]["weight"] += w
                    else:
                        G_.add_edge(u, v, weight=w)
                # replace the original
                G = G_
            else:
                raise ValueError("The specified graph type cannot be made. Choose one of 'simple', 'multi'.")
            
            nx.set_node_attributes(G, n.set_index('champ').to_dict('index'))
            
            self.network.append(G)

        self.index = 0
        self.link_type = link
        self.graph_type = gtype

    def __iter__(self):
        return self
    
    def __next__(self):
        if self.index > self.periods:
            raise StopIteration
        else:
            self.index += 1
            return self.network[self.index]
    
    def __getitem__(self, index):
        return self.network[index]

    @staticmethod
    def load(f_):
        """
        Load a TNetworkX object from an HDF5 file.

        Arguments:

        f_ -- an HDF5 file dumped by a TNetworkX object.
        """
        with h5py.File(f_, "r") as f:
            p = f["params"]
            n = f["network"]

            # create an empty network object
            obj = TNetworkX.__new__(TNetworkX)

            # read parameters
            obj.index = 0
            obj.periods = p.attrs["periods"]
            obj.link_type = p.attrs["link_type"]
            obj.graph_type = p.attrs["graph_type"]

            # read network
            obj.network = [nx.readwrite.json_graph.node_link_graph(json.loads(N.decode("ascii"))) 
                                for N in f["network"]]

            f.close()

        return obj


    def dump(self, f_):
        """
        Dump a TNetworkX object to an HDF5 file.

        Arguments:

        f_ -- An HDF5 file to be dumped to. Does not have to exist.
        """
        with h5py.File(f_, "w") as f:

            network = f.create_dataset("network", (self.periods,), dtype=h5py.special_dtype(vlen=bytes))
            x = [json.dumps(nx.readwrite.json_graph.node_link_data(N)).encode("ascii") 
                    for N in self.network]
            network[0:self.periods] = x

            # graph parameters
            params = f.create_group("params")
            params.attrs["periods"] = self.periods
            params.attrs["link_type"] = self.link_type
            params.attrs["graph_type"] = self.graph_type
            
            # file metadata
            info = f.create_group("metadata")
            info.attrs["uid"] = str(uuid.uuid4())
            info.attrs["creation_date"] = str(datetime.datetime.now())

            f.close()
    
    def gephi(self, d):
        """
        """
        pass