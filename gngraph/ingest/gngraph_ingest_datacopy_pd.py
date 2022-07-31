# Example python program to read data from a PostgreSQL table
# and load into a pandas DataFrame

import psycopg2
import pandas as pds
import json
from sqlalchemy import create_engine


import os,sys
curentDir = os.getcwd()
parentDir = curentDir.rsplit('/', 1)[0]
if parentDir not in sys.path:
    sys.path.append(parentDir)

from gngraph.config.gngraph_config import GNGraphConfig
from gngraph.gngraph_dbops.gngraph_pgresdbops import GNGraphPgresDBOps
from gngraph.gngraph_dbops.gngraph_staticfileops import GNGraphStaticFileOps
from gnappsrv.gn_config import gn_log, gn_log_err


class     GNGraphDataCopyOps:

    def __init__(self, fileargs, gdbargs):

        self.__fargs = fileargs
        self.__gdbargs = gdbargs
        #if (self.__fargs["ftype"] == "csv"):
            #try:
        #    self.__nodeDF = pds.read_csv(self.__fargs["fpath"], delimiter=self.__fargs["fdelim"])
            #except Exception as err:
            #    print(err)
        #    gn_log('GNIngest: file is parsed and created dataframe')

        ### Establish metadb conn and datadb conn    
        if (self.__gdbargs["gdbflag"]):
            self.gdb_conn_setup()

        if (self.__gdbargs["staticfiles"]):
            gngraph_folder = self.__gdbargs["gndatafolder"]+"/gngraph";
            self.__gngrp_sfops = GNGraphStaticFileOps(gngraph_folder)
            
        ## get config class
        if (self.__gdbargs["gndatafolder"]):
            id_cfg_path = self.__gdbargs["gndatafolder"]+"/gngraph/config"
            self.__gngrp_cfg = GNGraphConfig(id_cfg_path)

        ###set metanode columns for pgresdb and static files
        self.__metanode_columns=["gnnodeid", "gnnodename", "gnnodetype", "gnnodeprop", "uptmstmp"]
        self.__metaedge_columns=["gnedgeid", "gnedgename", "gnedgetype", "gnsrcnodeid", "gntgtnodeid", "gnedgeprop", "uptmstmp"]

        self.gnnodeparentid = -1
        self.gnnode_parent_name = self.__fargs["nodename"]
            
    def    gdb_conn_setup(self):

        with open(self.__gdbargs["gdbcredsfpath"], encoding="utf-8") as fh:
             gdb_creds = json.load(fh)

        self.__gdbMetaDBConnp = GNGraphPgresDBOps.from_args(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"], self.__gdbargs["gnmetaDB"], "gnmetadb")         

        self.__gdbDataDBConnp = GNGraphPgresDBOps.from_args(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"],  self.__gdbargs["gndataDB"], "gndatadb")

        


"""
 When DBmode is turned on, we update data from static files to database and turn searching using dbmode

"""
def     gngraph_datacopy_sf_to_pgres(gnDataFolder,gnGraphDBCredsFolder, gnCfgSettings):
    
    


    
if __name__ == "__main__":
    
    filename="salesorder.csv"
    ftype="csv"
    fdelim=','
    nodename="salesorder"
    bizdomain="sales_domain"
    gndata_folder="/home/jovyan/GnanaDiscover/GnanaPath/gndata"
    gngraph_creds_folder = "/home/jovyan/GnanaDiscover/GnanaPath/creds/gngraph"
    gngraph_ingest_file_api(filename, ftype, fdelim, nodename, bizdomain, gndata_folder, gngraph_creds_folder)



    
