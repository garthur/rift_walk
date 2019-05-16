# meta_graph.py

import os

import networkx as nx
import pandas as pd

from src.data import meta_ingest


class MemGraphData(object):
    
    def __init__(self, data_dict=None):
        if data_dict is None:
            self.nl = None
            self.el = None
            self.info = None
        elif isinstance(data_dict, dict):
            self.nl = data_dict["nl"]
            self.el = data_dict["el"]
            self.info = data_dict["info"]
        else:
            raise TypeError("The provided data dictionary is not a dictionary or None.")

    def read(self, filename):
        if isinstance(filename, str):
            self.nl = meta_ingest.gen_meta_nodelist(filename)
            self.el = meta_ingest.gen_meta_edgelist(filename)
            self.info = meta_ingest.gen_meta_info(filename)
        else:
            raise TypeError("The provided filename is not a string.")

    def subset(self):
        pass

    def collapse_nodelist(self):
        pass

    def collapse_edgelist(self):
        result = pd.DataFrame()
        
        for name, group in self.el.groupby(["champ_a", "champ_b"], as_index=False):
            new = pd.DataFrame(columns=["champ_a", "champ_b", "gameid", "games"])
            new.loc[0] = [group.champ_a.values[0], group.champ_b.values[0],
                        group.gameid.values.tolist(), len(group.index)]
            result = result.append(new)
        
        result = result.reset_index()
        return result

    def make_graph(self):
        result = nx.from_pandas_edgelist(
            self.collapse_edgelist(), "champ_a", "champ_b",
            ["gameid", "games"]
        )

        return result

class PostgreSQLGraphData(object):

    def __init__(self, filename, connection_info=None):
        if isinstance(filename, str):
            pass
        elif filename is None:
            pass
        else:
            raise TypeError("The provided filename is not None or a string.")