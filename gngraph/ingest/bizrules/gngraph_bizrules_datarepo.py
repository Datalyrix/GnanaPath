####################################################################
# GNGraph DB module does following tasks
#    - Add data relationships
#    - Add relationship attributes to node to neo4j
###################################################################
import os
import csv
import json
import sys
import pandas as pds
import numpy as np
import warnings

from moz_sql_parser import parse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit,split
from pyspark.sql import Row
from pyspark.sql.types import *



import re
warnings.simplefilter('ignore')
curentDir = os.getcwd()
parentDir = curentDir.rsplit('/', 1)[0]
gngraph_rootDir = parentDir.rsplit('/', 1)[0]



if parentDir not in sys.path:
    sys.path.append(parentDir)
if gngraph_rootDir not in sys.path:
    sys.path.append(gngraph_rootDir)

    
from config.gngraph_config import GNGraphConfig
from gngraph_dbops.gngraph_pgresdbops import GNGraphPgresDBOps
from gngraph_dbops.gngraph_staticfileops import GNGraphStaticFileOps
from ingest.bizrules.gngraph_bizrules_ops import GNGraphBizRuleOps
from search.gngraph_sqlparser import GNGraphSqlParserOps
from search.gngraph_search_main import gngrp_srch_qry_api

class      GNGraphBizRulesDataRepoOps:    

    def     __init__(self, fileargs, gdbargs, bizrid, spk):
        self.__fargs = fileargs;
        self.__gdbargs = gdbargs

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
        self.__metabizr_columns=["gnrelid", "gnrelname", "gnreltype", "gnsrcnodeid", "gntgtnodeid", "gnmatchnodeid", "gnrelprop", "gnrelobj", "state", "freq", "uptmstmp"]

        self.__spk = spk
        accessmode="pgres"
        
        self.__gn_bizr_metaops = GNGraphBizRuleOps( self.__fargs, gdbargs, accessmode)
        jstr = self.__gn_bizr_metaops.bizr_get_by_id(bizrid)

        #self.__gn_rel_json = json.loads(jstr)
        self.__gn_bizr_id = bizrid
        self.__gn_rel_json = jstr[0]
        print('BizrDatarepo: meta bizrinfo ')
        print(self.__gn_rel_json)
        
        gsql_p = self.__gn_rel_json["gnrelobj"]
        self.__gn_rel_msql = str(gsql_p["matchcondition"])
        self.__gn_rel_srcnode = str(gsql_p["srcnode"])
        self.__gn_rel_targetnode = str(gsql_p["targetnode"])
        self.__gn_rel_bizrname = str(gsql_p["rulename"])
        
        self.__gn_rsql_parsed = GNGraphSqlParserOps(self.__gn_rel_msql)
        
        entlist = self.__gn_rsql_parsed.get_entlist()
        reformatted_sql = self.__gn_rsql_parsed.gn_sqlp_reformat_qry()

        print('BizDataRepos: reformated sql '+reformatted_sql)

    def    gdb_conn_setup(self):
        
        with open(self.__gdbargs["gdbcredsfpath"], encoding="utf-8") as fh:
             gdb_creds = json.load(fh)

        self.__gdbMetaDBConnp = GNGraphPgresDBOps.from_args(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"], self.__gdbargs["gnmetaDB"], "gnmetadb")
        self.__gdbDataDBConnp = GNGraphPgresDBOps.from_args(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"],  self.__gdbargs["gndataDB"], "gndatadb")

    def    get_rel_json(self):
        return self.__gn_rel_json
    def    get_rel_srcnode(self):
        return self.__gn_rel_srcnode
    def    get_rel_targetnode(self):
        return self.__gn_rel_targetnode
    
    def    get_rsql_entlist(self):
        return self.__gn_rsql_parsed.get_entlist()
    def    get_rsql_entlist_alias(self):
        return self.__gn_rsql_parsed.get_entlist_alias()
    def    get_rsql_from_str(self):
        return self.__gn_rsql_parsed.get_from_str()
    def    get_rsql_where_str(self):
        return self.__gn_rsql_parsed.get_where_str()
    
    def    bizrule_rule_get(self, bizrid):

        if (self.__gncfg["gdbflag"] == 1):
            metabizr_obj = self.__gngrp_dbops.get_bizrule_metainfo(bizrid, self.__spk)
        elif (self.__gncfg["staticfiles"] == 1):
            metabizr_obj = self.__gngrp_sfops.get_bizrule_metainfo(bizrid, self.__spk)
        return metabizr_obj


    def     bizrule_rule_execute(self, bizrid):
                  
       print( 'bizrules_rule_execute: Starting for biz relid'+ str(bizrid) +' ')
       metabizr_obj = self.bizrule_rule_get(bizrid)
       print('GnGrpBizRuleDataRepoExecute: parsing matching cond '+metabizr_obj["gnrelobj"])

       gnsqlp = GNGraphSqlParserOps(metabizr_obj["gnrelobj"]) 



       

    def      bizr_datanodes_setup_api(self):

        ###entlist is list of nodesname that need to be mapped
        for ent in self.__entlist:
            entD = {}
            ent_metanode_info =  self.get_metanode_info(ent)
            jprop = json.loads(ent_metanode_info["gnnodeprop"])
            node_name = ent_metanode_info["gnnodename"]
            bizdomain = jprop["bizdomain"]

            print("GNGrpSrchMain:  setup api nodename "+node_name+" bizdomain:"+bizdomain)
            entnodeDF = self.get_datanode_mapped_df(node_name, bizdomain)
            if (entnodeDF is not None):
               ent_metanode_info["df"] = entnodeDF
               self.__gngrp_dnDFList.append(ent_metanode_info)
            else:
               print("gngrp_srch_setup: "+node_name+" nodeDF is empty ")

        return


    def   create_bizrnode_edges(self, dnjson):

         #gn_node_parent_id = self.get_metanode_parent_id()
         #gn_nodeid_max_c = self.__gngrp_cfg.get_nodeid_max()
         gn_edgeid_max_c = self.__gngrp_cfg.get_edgeid_max()
         gn_edgeid_c = gn_edgeid_max_c + 1
         
         ###self.__bizrDNodeDF = pds.DataFrame.from_dict(dnjson, orient="index")
         self.__bizrDNodeDF = pds.read_json(dnjson)
         self.__nodeEdgeDF = self.__bizrDNodeDF.copy()
         ####self.__nodeEdgeDF.rename( columns=({'gnnodeid':'gntgtnodeid', 'gnmetanodeid':'gnsrcnodeid'}), inplace=True)

         ## Add gnedgeid
         self.__nodeEdgeDF['gnedgeid'] = pds.RangeIndex(stop=self.__nodeEdgeDF.shape[0])+gn_edgeid_c

         #### Add gnedgename IS
         ##relname="IS"
         self.__nodeEdgeDF["gnedgename"] = self.__gn_rel_bizrname

         ### Add gnedgetype
         edgetype="GNBizRuleNodeEdge"
         self.__nodeEdgeDF["gnedgetype"] = edgetype

         #### Add gnedgeprop
         self.__nodeEdgeDF["gnedgeprop"] = self.__nodeEdgeDF["gntgtnodeid"].apply(lambda x: json.dumps({'gnbizrelid':str(self.__gn_bizr_id) }))

          #### Add gnedgeprop
          #####  self.__nodeEdgeDF["gnedgeprop"] = self.__nodeEdgeDF["gntgtnodeid"].apply(lambda x: json.dumps({'gntgtlabel':self.gnnode_parent_name+str(x), 'gnsrclabel':self.gnnode_parent_name,  "gnsrcdomain": "gnmeta", "gntgtnodeloc":   self.__fargs["nodename"], "gntgtdomain": self.__fargs["bizdomain"] }))

         
         #### Update utimestamp
         utmstmp = pds.Timestamp.now()
         self.__nodeEdgeDF['uptmstmp'] = utmstmp

         #####Select edgenode columns and prepare for write
         self.__gndatanodeEdgeDF = self.__nodeEdgeDF[["gnedgeid","gnedgename","gnedgetype","gnsrcnodeid","gntgtnodeid","gnedgeprop", "uptmstmp"]]

         print('GNGrpBizRuleOps: showing biznodes ')
         #self.__gndatanodeEdgeDF.columns

         ## write edges to database
         if (self.__gdbargs["gdbflag"]):
            self.__gdbMetaDBConnp.metadb_edges_write(self.__gndatanodeEdgeDF)
         if (self.__gdbargs["staticfiles"]):
             self.__gndatanodeEdgeDF["uptmstmp"] = self.__gndatanodeEdgeDF["uptmstmp"].astype(str)
             self.__gngrp_sfops.metadb_edges_append_write(self.__gndatanodeEdgeDF)

         #save edgeid max
         gn_edgeid_max_n = self.__gndatanodeEdgeDF.shape[0]+gn_edgeid_c-1
         self.__gngrp_cfg.save_edgeid_max(gn_edgeid_max_n)

        
####################################
    

               
def        gngrp_bizr_rule_execute_api(bizrid, spark, gndata_folder, gngraph_creds_folder):

        gdb_creds_filepath=gngraph_creds_folder+"/gngraph_pgres_dbcreds.json"
        fileargs = {}
        gdbargs = {}
        gdbargs["gdb"] = "pgres"
        gdbargs["gdbflag"] = 1
        gdbargs["gdbcredsfpath"] = gdb_creds_filepath
        gdbargs["gnmetaDB"] = "gngraph_db"
        gdbargs["gndataDB"] = "gngraph_db"
        gdbargs["staticfiles"] = 1
        gdbargs["staticfpath"] = gndata_folder+"/uploads";
        gdbargs["gndatafolder"] = gndata_folder

        fargs = {}
        fargs["gngraphfolder"] = gndata_folder+"/gngraph"
        fargs["gnmetanodesfname"] = "gnmeanodes.json"
        fargs["gnmetaedgesfname"] = "gnmetaedges.json"

        accessmode="files"
        
        ##entlist = gngrph_srch_get_entlist(sqlst)
        gngrp_bizr_ops = GNGraphBizRulesDataRepoOps(fargs, gdbargs, bizrid, spark)
       
        ##gnsrch_ops = GNgraphSearchOps(gndata_folder, accessmode, fargs, gdbargs, gngrp_bizr_ops.__entlist, spark)
        ##gnsrch_ops.gngraph_search_setup_api()
        gndata_folder="/home/jovyan/GnanaDiscover/GnanaPath/gndata"
        gngraph_creds_folder = "/home/jovyan/GnanaDiscover/GnanaPath/creds/gngraph"

        ### Set spark session
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        rel_json = gngrp_bizr_ops.get_rel_json()
        srcnode = gngrp_bizr_ops.get_rel_srcnode()
        targetnode = gngrp_bizr_ops.get_rel_targetnode()
        print('BizrDataRepo: rel json ')
        print(rel_json)
        print(srcnode)
        print(targetnode)

        
        rjson_sql = ""
        ### Prepare sql statement to get gnsrcnodeid, gntgtnodeid so that new edge relation can be created
        ent_list = gngrp_bizr_ops.get_rsql_entlist()
        ent_alias_list = gngrp_bizr_ops.get_rsql_entlist_alias()
        frm_str = gngrp_bizr_ops.get_rsql_from_str()
        where_str = gngrp_bizr_ops.get_rsql_where_str()
        
        print('BizrDataRepo: RSQL is parsed ')
        print('****************************')
        print('Ent list')
        print(ent_list)
        print('Ent Alias List')
        print(ent_alias_list)
        print('From str ')
        print(frm_str)
        print('where str ')
        print(where_str)

    
        
        tent_list = ""
        i=0
        for e in ent_list:
            if (e == srcnode):
                src_idx = i
            if (e == targetnode):
                tgt_idx = i

            i = i+1

        if (ent_alias_list[src_idx] is None):
            tent_list += ent_list[src_idx]+"."+"gnnodeid as gnsrcnodeideid "
        else:
            tent_list += ent_alias_list[src_idx]+"."+"gnnodeid as gnsrcnodeid "

        tent_list += ", " 
            
        if (ent_alias_list[tgt_idx] is None):
            tent_list += ent_list[tgt_idx]+"."+"gnnodeid as gntgtnodeideid "
        else:
            tent_list += ent_alias_list[tgt_idx]+"."+"gnnodeid as gntgtnodeid"

        rjson_sql += "Select "+tent_list+" "
        rjson_sql += frm_str+ " "
        rjson_sql += where_str+ " "
        print('***********************************')
        print('BizrDataRepo: processed rSQL')
        print(rjson_sql)
        nodesonly = 1    
        (nJSON, eJSON) = gngrp_srch_qry_api(rjson_sql, spark, gndata_folder, gngraph_creds_folder, nodesonly)

        nfile="bnodes.json"
        with open(nfile, 'w') as fp:
            json.dump(nJSON, fp)

        nJSONStr = json.dumps(nJSON)    
        ### Add Edges to bizrules
        gngrp_bizr_ops.create_bizrnode_edges(nJSONStr)

        ## We need to create edges between src node and target node
        return (nJSON, eJSON)
        
       

if __name__ == '__main__':

    print("Starting gn ingest file")
    curDir = os.getcwd()
    rtDir = curDir.rsplit('/', 1)[0]
    app_name="gngraph"

    if rtDir not in sys.path:
        sys.path.append(rtDir)

    gndata_folder="/home/jovyan/GnanaDiscover/GnanaPath/gndata"
    gngraph_creds_folder = "/home/jovyan/GnanaDiscover/GnanaPath/creds/gngraph"
    
    bizrid = 900001    
    ### Set spark session
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    (nJSON, eJSON) = gngrp_bizr_rule_execute_api(bizrid, spark, gndata_folder, gngraph_creds_folder)
    rfile="bizr_nodes.json"
    with open(rfile, 'w') as fp:
            json.dump(nJSON, fp)


    efile="bizr_edges.json"
    with open(efile, "w") as fp:
            json.dump(eJSON, fp)



