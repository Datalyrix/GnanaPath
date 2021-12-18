"""
 Database Clone Operations Main Class
  This class provides methods for clone operations of graph data 
  - from static files to postgresdb (on initialization and new database creation)
  - from postgres database to staticfile    

"""

import psycopg2
import pandas as pds
import json
from sqlalchemy import create_engine

import os,sys
curentDir = os.getcwd()
parentDir = curentDir.rsplit('/', 1)[0]
rootDir = parentDir.rsplit('/', 1)[0]

if parentDir not in sys.path:
    sys.path.append(parentDir)
if rootDir not in sys.path:
    sys.path.append(rootDir)
    
print(' Root directory '+rootDir)    
print(' Parent directory '+parentDir)
    
from gngraph.config.gngraph_config import GNGraphConfig
from gngraph.gngraph_dbops.gngraph_pgresdbops import GNGraphPgresDBOps
from gngraph.gngraph_dbops.gngraph_staticfileops import GNGraphStaticFileOps
from gnappsrv.gn_config import gn_log, gn_log_err
from gnappsrv.gn_config import gn_pgresdb_getconfiguration

class     GNGraphDataCloneOps:

    def __init__(self, gdbargs):

        ###self.__fargs = fileargs
        self.__gdbargs = gdbargs
        print(gdbargs)
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

        #with open(self.__gdbargs["gdbcredsfpath"], encoding="utf-8") as fh:
        #     gdb_creds = json.load(fh)
        #     print(' conn setup ')
        #     print(gdb_creds)
        gdb_creds = gn_pgresdb_getconfiguration(self.__gdbargs["gdbcredsfolder"])
        print(gdb_creds)
        self.__gdbMetaDBConnp = GNGraphPgresDBOps.from_args(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"], self.__gdbargs["gnmetaDB"], "gnmetadb")         

        self.__gdbDataDBConnp = GNGraphPgresDBOps.from_args(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"],  self.__gdbargs["gndataDB"], "gndatadb")

    
    def    upload_metainfo(self, from_mode):
         
        if from_mode == "staticfiles":
            self.__metaNodeDF = self.__gngrp_sfops.metadb_load_metanode_df()
            self.__metaNodeDF.head(10)

"""
 When DBmode is turned on, we update data from static files to database and turn searching using dbmode

"""
def     gngraph_dataclone_sf_to_pgres(gndata_folder, gngraph_creds_folder, gncfg_settings):

    gdb_creds_filepath=gngraph_creds_folder+"/gngraph_pgres_dbcreds.json"
    fileargs = {}

    print(' gncfg settings ')
    print(gncfg_settings)
    print('file args ')
    print(fileargs)
    gdbargs = {}
    gdbargs["gdb"] = "pgres"
    gdbargs["gdbflag"] = gncfg_settings["dbmode"]
    gdbargs["gdbcredsfolder"] = gngraph_creds_folder
    ###gdbargs["gdbcredsfpath"] = gdb_creds_filepath
    gdbargs["gnmetaDB"] = "gngraph_db"
    gdbargs["gndataDB"] = "gngraph_db"
    gdbargs["staticfiles"] = 1
    gdbargs["staticfpath"] = gndata_folder+"/uploads";
    gdbargs["gndatafolder"] = gndata_folder
    
    gngrph_clone_ops = GNGraphDataCloneOps(gdbargs)    
    frommode="staticfiles"
    tomode="pgres"
    gngrph_clone_ops.upload_metainfo(frommode)
    
        
  
    
    
if __name__ == "__main__":
    
    gndata_folder="/opt/gnpdev/gndata"
    gngraph_creds_folder = "/opt/gnpdev/creds/gngraph"
    gncfg_settings = {}
    gncfg_settings["dbmode"] = 1
    gncfg_settings["sfmode"] = 1
    
    gngraph_dataclone_sf_to_pgres(gndata_folder, gngraph_creds_folder, gncfg_settings)
    
