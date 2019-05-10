# meta_graph.py

import os

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

class PostgreSQLGraphData(object):

    def __init__(self, filename, connection_info=None):
        if isinstance(filename, str):
            pass
        elif filename is None:
            pass
        else:
            raise TypeError("The provided filename is not None or a string.")