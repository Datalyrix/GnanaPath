####################################################################
# GnanaWarehouse_DB module does following tasks
#    - Add meta relationships
#    - Add meta attributes to node to neo4j
###################################################################

import os
import csv
import json
import sys
import numpy as np
import warnings
import pandas as pds

import os,sys
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


import re
warnings.simplefilter('ignore')

"""
Biz rules meta repo Pandas-based

"""

class     GNGraphBizRuleOps:

    def     __init__(self, fileargs, gdbargs, accessmode):
        self.__fargs = fileargs;
        self.__gdbargs = gdbargs
        self.__gncfg = {}


        if (accessmode == "files"):
          self.__gncfg["staticfiles"] = 1
          self.__gncfg["gdbflag"] = 0
        else:
          self.__gncfg["staticfiles"] = 0
          self.__gncfg["gdbflag"] = 1

          
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
    

        
    def    gdb_conn_setup(self):

        with open(self.__gdbargs["gdbcredsfpath"], encoding="utf-8") as fh:
             gdb_creds = json.load(fh)

        self.__gdbMetaDBConnp = GNGraphPgresDBOps.from_args(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"], self.__gdbargs["gnmetaDB"], "gnmetadb")

        self.__gdbDataDBConnp = GNGraphPgresDBOps.from_args(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"],  self.__gdbargs["gndataDB"], "gndatadb")


        
           
    def      bizrules_metarepo_metanode_chk(self, nodename):
        nodeid = None
        if (self.__gdbargs["gdbflag"]):
            nodeid = self.__gdbMetaDBConnp.metadb_metanode_chk_nodeid(nodename)
            
        if (self.__gdbargs["staticfiles"]):
            nodeid = self.__gngrp_sfops.metadb_metanode_id_get(nodename)
        return nodeid
        
    def      bizrule_verify_rules(self, rjson):

      fail = 0
      # Verify if srcnode exists in metarepo
      print("gngrp_bizrules_verify: verify rules")
      if (rjson["srcnode"]):
        if (verbose > 2):
            print('gngrp_bizrules_rule_verify: verify if srcnode exists:' + rjson['srcnode'])
        l = self.gngrp_metarepo_metanode_check(rjson['srcnode'])
        if (l == 0):
            fail = fail + 1
      else:
        fail = fail + 1

      if (rjson["targetnode"]):
        if (verbose > 2):
            print(
                'gndw_rwl_verify_rules: verify if tgtnode exists:' +
                rjson['targetnode'])
        l = self.gndw_metarepo_metanode_check(rjson['targetnode'])

        if (l == 0):
            fail = fail + 1
      else:
        fail = fail + 1

      if (rjson["matchnode"]):

        print('gngraph_rel_verify_rules: verify if matchnode exists:' +
                rjson['matchnode'])
        l = self.gndw_metarepo_metanode_check(rjson['matchnode'])
        if (l == 0):
            fail = fail + 1
      else:
        fail = fail + 1

      return fail

    

    def     bizrules_rule_metarepo_add(self, rjson):
        #srcnodeid = self.bizrules_metarepo_metanode_chk(rjson["srcnode"])
        #tgtnodeid = self.bizrules_metarepo_metanode_chk( rjson["targetnode"])
        #matchnodeid = self.bizrules_metarepo_metanode_chk( rjson["matchnode"])
        (found, srcnodeid, tgtnodeid, matchnodeid) = self.bizrule_rel_rule_exists(rjson)
        if (found != -1):
            return -1
        
        gn_bizrule_rel_id = self.__gngrp_cfg.get_bizr_relid_max()+1
        
        rulename = rjson['rulename']

        reltype="GNBizRule"
        freq="daily"
        state="Active"
        up_tmstmp = pds.Timestamp.now()
        bizrelprop = {"bizrulelabel": rjson["rulename"]}
        bizrelprop_str = json.dumps(bizrelprop)
        bizrel_json_str = json.dumps(rjson)
        
        bizrule_node_e = [gn_bizrule_rel_id, rjson["rulename"], reltype, srcnodeid, tgtnodeid, matchnodeid, bizrelprop_str, bizrel_json_str, state, freq, up_tmstmp]
        self.__metaBizRuleDF = pds.DataFrame([bizrule_node_e], columns=self.__metabizr_columns)                     
        print("gngrph_bizrule_add: Adding BizRule Node")

        ###rfound = gndw_rel_rules_bizrule_relation_exists(rulename)

        if (self.__gdbargs["gdbflag"]):
           self.__gdbMetaDBConnp.metadb_bizrules_write(self.__metaBizRuleDF)

        if (self.__gdbargs["staticfiles"]):
            print("gngrp_bizrules_add: write rules to static files ")
            self.__metaBizRuleDF["uptmstmp"] = self.__metaBizRuleDF["uptmstmp"].astype(str)
            self.__gngrp_sfops.metadb_bizrules_append_write(self.__metaBizRuleDF)
        self.__gngrp_cfg.save_bizr_relid_max(gn_bizrule_rel_id)
  

                
    def     bizrule_rel_rule_exists(self, rjson):
          # Check if Biz Rule Rulename exists
          srcnode = rjson["srcnode"]
          tgtnode = rjson["targetnode"]
          matchnode = rjson["matchnode"]
          rulename = rjson["rulename"]

          srcnodeid = self.bizrules_metarepo_metanode_chk(rjson["srcnode"])
          tgtnodeid = self.bizrules_metarepo_metanode_chk( rjson["targetnode"])
          matchnodeid = self.bizrules_metarepo_metanode_chk( rjson["matchnode"])

          if (srcnodeid is None):
              return (-1, None, None, None)
          
          if (matchnodeid is None):
              return (-1, None, None, None)
          
          relid = -1

          if (self.__gdbargs["gdbflag"]):
             relid = self.__gdbMetaDBConnp.metadb_bizrules_rule_chk(srcnodeid, rulename)            

          if (self.__gdbargs["staticfiles"]):
             relid = self.__gngrp_sfops.metadb_bizrules_rule_chk(srcnodeid, rulename)

          return (relid, srcnodeid, tgtnodeid, matchnodeid)

    def  bizr_get_by_id(self, bizrid):
          if (self.__gncfg["staticfiles"] == 1):
              bizr_json = self.__gngrp_sfops.metadb_bizrules_bizr_get(bizrid)

          if (self.__gncfg["gdbflag"] == 1):
              bizr_json = self.__gdbMetaDBConnp.metadb_bizrules_bizr_get(bizrid)
          return bizr_json

      
# API to add business rules to graphdb
def    gngraph_bizrules_rule_add_api(rjson, gndata_folder, gngraph_creds_folder):

        print('gngrp_bizrules_add: adding bizrules to meta and data repo')

        gdb_creds_filepath=gngraph_creds_folder+"/gngraph_pgres_dbcreds.json"

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

        accessmode="pgres"

        gnbizr_ops = GNGraphBizRuleOps(fargs, gdbargs, accessmode)

        ret = gnbizr_ops.bizrules_rule_metarepo_add(rjson)
        ###ret = gnbizr_ops.bizrules_rule_datarepo_add(rjson)
        
        return ret


if __name__ == '__main__':

    rfilepath = "./rel_ex.json"
    gndata_folder="/home/jovyan/GnanaDiscover/GnanaPath/gndata"
    gngraph_creds_folder = "/home/jovyan/GnanaDiscover/GnanaPath/creds/gngraph"

    with open(rfilepath) as rfile:
        rjson = json.load(rfile)
        # matchcondition: "(salesorder.customerid \= customer.customerid) AND
        # (salesorder.productid = product.productid)" }'

        ret = gngraph_bizrules_rule_add_api(rjson, gndata_folder, gngraph_creds_folder)
