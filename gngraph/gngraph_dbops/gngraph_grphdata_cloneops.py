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
from gngraph.gngraph_dbops.gngraph_pgres_dbmgmtops import GNGraphPgresDBMgmtOps
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
        ##self.__metanode_columns=["gnnodeid", "gnnodename", "gnnodetype", "gnnodeprop", "uptmstmp"]
        ##self.__metaedge_columns=["gnedgeid", "gnedgename", "gnedgetype", "gnsrcnodeid", "gntgtnodeid", "gnedgeprop", "uptmstmp"]

        ##self.gnnodeparentid = -1
        ##self.gnnode_parent_name = self.__fargs["nodename"]
            
    def    gdb_conn_setup(self):

        #with open(self.__gdbargs["gdbcredsfpath"], encoding="utf-8") as fh:
        #     gdb_creds = json.load(fh)
        #     print(' conn setup ')
        #     print(gdb_creds)
        gdb_creds = gn_pgresdb_getconfiguration(self.__gdbargs["gdbcredsfolder"])
        print(gdb_creds)
        self.__pgresDBConnp = GNGraphPgresDBOps.from_args(gdb_creds["serverIP"], gdb_creds["serverPort"], gdb_creds["username"], gdb_creds["password"], gdb_creds["dbname"], "gnmetadb")         

        ###self.__gdbDataDBConnp = GNGraphPgresDBOps.from_args(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"],  self.__gdbargs["gndataDB"], "gndatadb")

        ###self.__pgresDBMgmtConnp = GNGraphPgresDBMgmtOps.from_args(gdb_creds["serverIP"], gdb_creds["serverPort"], gdb_creds["username"], gdb_creds["password"], gdb_creds["dbname"], "gnmetadb")
        ##self.__pgresDBMgmtConnp.db_connect()
        
    def    upload_metainfo(self, from_mode):
         
        if from_mode == "staticfiles":
            self.__metaNodeDF = self.__gngrp_sfops.metadb_load_metanode_df()
            self.__metaNodeDF.count()
            gn_log('GnGrphCloneOps: metnode data is uploaded ')
            
            if self.__metaNodeDF is None:
                return -1
            
            

    def    write_metaDF(self, to_mode):

        if to_mode == "pgres":
            if  self.__metaNodeDF is not None:
                self.__pgresDBConnp.metadb_nodes_write(self.__metaNodeDF)


    def    upload_metaedge_info(self, from_mode):
         
        if from_mode == "staticfiles":
            self.__metaEdgeDF = self.__gngrp_sfops.metadb_load_metaedge_df()
            gn_log("GnGrphCloneOps: metaedges data is uploaded ")
            if self.__metaEdgeDF is None:
                return -1            

            
    def    write_metaEdgeDF(self, to_mode):
        if to_mode == "pgres":
            if  self.__metaEdgeDF is not None:
                self.__pgresDBConnp.metadb_edges_write(self.__metaEdgeDF)

    def    get_datanode_data(self, bizdomain, nodename, from_mode):

        if from_mode == "staticfiles":
            dataNodeDF = self.__gngrp_sfops.datadb_load_metanode_df(bizdomain, nodename)
            return dataNodeDF

        
    def    create_datanode_table(self, bizdomain, nodename, to_mode):
        
        if  to_mode == "pgres":
            self.__pgresDBConnp.grphdb_create_table(nodename, bizdomain)
            
            print('GnMgmtClone: creating datanode table '+nodename)
            
    def    write_datanode_data(self, dnodeDF, bizdomain, node_table, to_mode):
        ret = -1
        if  to_mode == "pgres":
            if  dnodeDF is not None:
                tgt_schema = bizdomain
                print('Writing Data node to db node: '+node_table)
                ret = self.__pgresDBConnp.datadb_nodes_write(dnodeDF, tgt_schema, node_table)
                return ret
        return ret
    
    def    get_metanodes(self):

        list = []
        if self.__pgresDBConnp is not None:
            nDF = self.__pgresDBConnp.metadb_nodes_get_metanodes(1)
            list = nDF.values.tolist()
                                        
        return list

    
                
"""
 When DBmode is turned on, we update data from static files to database and turn searching using dbmode

"""
def     gngraph_dataclone_op(frommode, tomode, gndata_folder, gngraph_creds_folder, gncfg_settings):

    gdb_creds_filepath=gngraph_creds_folder+"/gngraph_pgres_dbcreds.json"
    fileargs = {}

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
    #frommode="staticfiles"
    #tomode="pgres"
    gngrph_clone_ops.upload_metainfo(frommode)
    gn_log('GnGrphCloneOps: copying metanodes from '+frommode+' to '+tomode)
    gngrph_clone_ops.write_metaDF(tomode)     

    ### Now write edge
    gngrph_clone_ops.upload_metaedge_info(frommode)
    gn_log('GNGrphCloneOps: copying metaedges from '+frommode+' to '+tomode)
    gngrph_clone_ops.write_metaEdgeDF(tomode)

    ##get all metanodes
    nodelist = gngrph_clone_ops.get_metanodes()

    ## list for each node and load data into data tables
    for  node in nodelist:
        
        gnnodename = node[1]
        gnnodetype = node[2]
        gnnodeprop = node[3]
        gn_log("GnGrphCloneOps: copying datanode "+gnnodename+"  from  "+frommode+" to "+tomode)
        gn_log("GnGrphCloneOps: data for bizdomain:"+gnnodeprop["bizdomain"]+" node:"+gnnodename)
        
        dNodeDF = gngrph_clone_ops.get_datanode_data(gnnodeprop["bizdomain"], gnnodename, frommode)

        if tomode == "pgres":
           gngrph_clone_ops.create_datanode_table(gnnodeprop["bizdomain"], gnnodename, tomode)
           gn_log("GnGrphCloneOps: datanode table created  ")
           gn_log("GnGrphCloneOps: copying datanode "+gnnodename+" data from "+frommode+" to "+tomode)
           ret = gngrph_clone_ops.write_datanode_data(dNodeDF, gnnodeprop["bizdomain"], gnnodename, tomode)
           if ret == -1:
               gn_log("GnGrpCloneOps: DB copying data for node "+gnnodename+"  FAILED ")
           else:
               gn_log("GnGrphCloneOps: DB copying data for node "+gnnodename+"  SUCCESS")


               
if __name__ == "__main__":
    
    gndata_folder="/opt/gnpdev/gndata"
    gngraph_creds_folder = "/opt/gnpdev/creds/gngraph"
    gncfg_settings = {}
    gncfg_settings["dbmode"] = 1
    gncfg_settings["sfmode"] = 1
    
    gngraph_dataclone_sf_to_pgres(gndata_folder, gngraph_creds_folder, gncfg_settings)
    
