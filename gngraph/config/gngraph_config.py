import pickle
import os.path


class GNGraphConfig:

    def   __init__(self, configpath):
        self.gndata_graph_config_folder = configpath
        self.gnnode_id_fpath = configpath+"/gnnode_id.pkl"
        self.gnedge_id_fpath = configpath+"/gnedge_id.pkl"
        

    def save_nodeid_max(self, node_id_val):
        #data_file_name="gnnodeid.pkl"
        with open(self.gnnode_id_fpath, 'wb') as fs:
           pickle.dump(node_id_val, fs)

    def get_nodeid_max(self):
        #data_file_name="gnnodeid.pkl"
        if  not os.path.exists(self.gnnode_id_fpath):
            return 100000
        with open(self.gnnode_id_fpath, 'rb') as fs:
            return pickle.load(fs)
    
    def save_edgeid_max(self,edgeid_val):
        ##data_file_name="gnedgeid.pkl"
        with open(self.gnedge_id_fpath, 'wb') as fs:
           pickle.dump(edgeid_val, fs)

    def get_edgeid_max(self):
        ###data_file_name="gnedgeid.pkl"
        if not os.path.exists(self.gnedge_id_fpath):
            return 500000
        with open(self.gnedge_id_fpath, 'rb') as fs:
            return pickle.load(fs)
