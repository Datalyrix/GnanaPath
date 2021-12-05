# Prepare the configuration file to be used with spark
# Get the upload path and the filename 
# TODO: Get the filetype and name once the user uploads
import os
from os import path
import sys
import json
import pathlib
import pickle
import numpy as np
import chardet
from threading import Thread
from queue import Queue
import time
import socket
from moz_sql_parser import parse
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit,split
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col,from_json


import os,sys
curentDir = os.getcwd()
parentDir = curentDir.rsplit('/', 1)[0]
if parentDir not in sys.path:
    sys.path.append(parentDir)

pparentDir = parentDir.rsplit('/', 1)[0]
if pparentDir not in sys.path:
    sys.path.append(pparentDir)

print(' Parent Dir ' + pparentDir)

from gngraph.config.gngraph_config import GNGraphConfig
from gngraph.gngraph_dbops.gngraph_pgresdbops_srch import GNGraphSrchPgresDBOps
from gngraph.gngraph_dbops.gngraph_staticfileops_srch import GNGraphSrchStaticFileOps
from gngraph.search.gngraph_sqlparser import GNGraphSqlParserOps
from gnappsrv.gn_config import gn_log, gn_log_err


class     GNGraphSearchOps:

    gncfg = {
            'app_name': 'gngraph-search',
            'gngraph_root_dir':'',
            'gngraph_uploads_dir':'',
            'gngraph_cfg_dir': '',
            'gngraph_cfg_filename': '',
            'gngraph_cfg_nodeid_filename': '',
            'gngraph_cfg_edgeid_filename': '',
            'gngraph_data_dir': '', 
            'gngraph_metanode_filename': '',
            'gngraph_metanodeattr_filename': '',
            'gngraph_edge_filename': '',
     }
    

    def __init__(self,  gngrp_datadir, accessmode, fargs, dbargs, sp):

        ###Set up config init routine
        self.__gncfg = {}
        self.__gncfg_fargs = {}
        self.__spark = sp
        self.__entlist = []
        self.__init_data = 0
        self.__init_meta = 0
        self.__gngrp_dnDFList = []
        self.gngraph_config_init(gngrp_datadir, accessmode, fargs, dbargs)
        print("GnSrchOps:  Search Initialization complete SUCCESS ")

        
    def     srch_init_data_status(self):
        return self.__init_data;

    def     srch_init_meta_status(self):
        return self.__init_meta;
    
    def     gngraph_config_init(self, gngrp_datadir, accessmode, fargs, dbargs):
       # check if the config_file exists 
       gngraph_path = gngrp_datadir+"/gngraph"

       self.__gncfg_accessmode = accessmode
       if (self.__gncfg_accessmode['sfmode'] == 1):
          self.__gncfg["staticfiles"] = 1
       if (self.__gncfg_accessmode['dbmode'] == 1):
          self.__gncfg["gdbflag"] = 1
          
       self.__gncfg["gngraph_root_dir"] = gngraph_path
       self.__gncfg["gngraph_cfg_dir"] = gngraph_path+"/config"
       self.__gncfg_fargs["gngraph_data_dir"] = self.__gncfg["gngraph_root_dir"]+"/data"
       self.__gncfg["gngraph_cfg_filename"] = "gngraph_config.json"
       self.__gncfg_fargs["gngraph_metanode_filename"] = "gnmetanodes.json"
       self.__gncfg_fargs["gngraph_metanodeattr_filename"] = "gnmetanodeattrs.json"
       self.__gncfg_fargs["gngraph_edge_filename"] = "gnmetaedges.json"
       self.__gncfg_fargs["gngraph_node_filename"] = "gnmetanodes.json"

       if (self.__gncfg_accessmode['sfmode'] == 1):
          self.__gngrp_sfops = GNGraphSrchStaticFileOps(self.__gncfg["gngraph_root_dir"], self.__spark)
          self.meta_edge_filepath = self.__gncfg_fargs["gngraph_data_dir"]+"/"+self.__gncfg_fargs["gngraph_edge_filename"]
          self.meta_node_filepath = self.__gncfg_fargs["gngraph_data_dir"]+"/"+self.__gncfg_fargs["gngraph_node_filename"]

          
       if (self.__gncfg_accessmode['dbmode'] == 1):
            self.__gncfg_dbargs = dbargs
            with open(self.__gncfg_dbargs["gdbcredsfpath"], encoding="utf-8") as fh:
                gdb_creds = json.load(fh)           
            self.__gngrp_dbops = GNGraphSrchPgresDBOps.from_args(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"], self.__spark)
            
              
    
    def    gngraph_cfgdata_write(self):
       cfgfpath = self.__gncfg["gngraph_cfg_dir"]+"/"+self.__gncfg["gngraph_cfg_filename"]
       print('gngraph_ingest: gngraph_config init:'+cfgfpath) 
       with open(cfgfpath,"w") as out_file:
           json.dump(self.__gncfg["configdata"], out_file)

    # If config file already exists read the data
    def    gngraph_cfgdata_read(self):
        config_file = self.__gncfg["gngraph_cfg_dir"]+"/"+self.__gncfg["gngraph_cfg_filename"]
        print('gngraph_ingest: gngraph_config init:'+cfgfpath) 
        with open(path,'r') as config_file:
            self.__gncfg["configdata"] = json.load(config_file)
                   
    def    gngraph_fileconvert_to_utf8(self, spath, destpath, sencode, dencode):
        #destpath = path+".utf8"
        #dest1 = "utf8-{0}".format(path)
        
        try:
             with open(spath, 'r', encoding=sencode) as inp,\
                  open(destpath, 'w', encoding=dencode) as outp:
                for line in inp:
                    outp.write(line)
        
           ### with open(spath, "rb") as source:
             ##   with open(destpath, "wb") as dest:
            ###     dest.write(source.read().decode(sencode).encode(dencode))
        except FileNotFoundError:
            print(" That file doesn't seem to exist.") 
      
    
    def    gngraph_fileencode_get(self, file_path):
        
         with open(file_path, 'rb') as rawdata:
           sres = chardet.detect(rawdata.read(100000))
         return sres["encoding"]
        

    #Read datanode  files and create dateframe object
    def   gngraph_datanodefile_read(self, spark, file_path):
        #df=spark.read.csv(file_path, header=True, inferSchema=True)
        
        with open(file_path, 'rb') as rawdata:
           sres = chardet.detect(rawdata.read(100000))
  
        print("gngraph: Input data file is encoded "+sres['encoding'])
        ###.option("encoding", sres["encoding"]) \
        ###.option("encoding", "utf-8") \
        df = spark.read.json(file_path)
               ### .option("encoding", sres["encoding"]) \
                                  
        return df


    # return the header 
    def get_columns(self, df):
         return df.columns

    # return column count
    def get_column_count(self,df):
        return len(df.columns)

    def get_file_name(self, path_val):
        return path_val[:-4]

    
 
    # write/update the meta node data to gnmeta[filename]node.json
    def   get_metanode_info(self, ent_name):

        if (self.__gncfg_accessmode["dbmode"] == 1):
            metanode_jobj = self.__gngrp_dbops.get_metanode_info(ent_name, self.__spark) 
        elif (self.__gncfg_accessmode["sfmode"] == 1):
           metanode_jobj = self.__gngrp_sfops.get_metanode_info(ent_name, self.__spark)
        else:
           metanode_jobj = {}

        return metanode_jobj   

    
    def    get_datanode_mapped_df(self, node_name, bizdomain):

         if (self.__gncfg_accessmode["dbmode"] == 1):
             dnodeDF = self.__gngrp_dbops.get_datanode_mapped_df(node_name, bizdomain, self.__spark)                
         elif (self.__gncfg_accessmode["sfmode"] == 1):
             dnodeDF = self.__gngrp_sfops.get_datanode_mapped_df(node_name, bizdomain, self.__spark)             
         else:
            dnodeDF = None

         return dnodeDF
                    

    def    get_metaedges_mapped_df(self):

        if (self.__gncfg_accessmode["dbmode"] == 1):
            self.__gnmetaEdgeDF = self.__gngrp_dbops.gnmetaedges_map_df(self.__spark)             
        elif (self.__gncfg_accessmode["sfmode"] == 1):
            self.__gnmetaEdgeDF = self.__gngrp_sfops.gnmetaedges_map_df(self.__spark)
        else:
            self.__gnmetaEdgeDF = None

        if (self.__gnmetaEdgeDF is None):
            gn_log('GNGrphSrchOps: metaEdgeDF is none')
            self.__init_meta = 0
        else:
            gn_log('GNGrphSrchOps: metaEdgeDF is mapped')
            self.__init_meta = 1

        return


     
    def    get_metanodes_mapped_df(self):

        if (self.__gncfg_accessmode["dbmode"] == 1):
             self.__gnmetaNodeDF= self.__gngrp_dbops.gnmetanodes_map_df(self.__spark)             
        elif (self.__gncfg_accessmode["sfmode"] == 1):
             self.__gnmetaNodeDF = self.__gngrp_sfops.gnmetanodes_map_df(self.__spark)
        else:
            self.__gnmetaNodeDF = None

        if (self.__gnmetaNodeDF is None):
            gn_log("GNGrphSrchOps: metaNodesDF is none")
            self.__init_meta = 0
        else:
            gn_log("GNGrphSrchOps: metaNodesDF is mapped")
            self.__init_meta = 1
        return


     
    def      gngraph_search_setup(self, sqlst):

        gn_srch_sql = sqlst  
        print("GnSrchOps: Parsing sql st "+gn_srch_sql)  
        gn_ssql_parsed = GNGraphSqlParserOps(gn_srch_sql)
        
        #self.__entlist
        t = gn_ssql_parsed.get_entlist()
        aEntList = []
        for x in t:
            if x not in self.__entlist:
               aEntList.append(x)
               
        print('GnSrchOps: search setup data entities list parsed ')
        print(aEntList)
        
        gn_ssql_parsed_where_str = gn_ssql_parsed.get_where_str()
        ###self.__gngrp_dnDFList = []
        ###entlist is list of nodesname that need to be mapped
        for ent in aEntList:            
            entD = {}                            
            ent_metanode_info =  self.get_metanode_info(ent)
            print('GnSrchOps: got metanode info for '+ent+' ')
            print(ent_metanode_info)
            if (len(ent_metanode_info) == 0):
                print('GnSrchOps: node '+ent+' does not exist ')
                errmsg = f"  node "+ent+" does not exist "
                return (-1, errmsg)
            ##jprop = json.loads(ent_metanode_info["gnnodeprop"])
            node_name = ent_metanode_info["gnnodename"]
            bizdomain = ent_metanode_info["bizdomain"]

            print("GnSrchOps:  setup api nodename "+node_name+" bizdomain:"+bizdomain)
            entnodeDF = self.get_datanode_mapped_df(node_name, bizdomain)

            if (entnodeDF is not None):
               ent_metanode_info["df"] = entnodeDF
               self.__gngrp_dnDFList.append(ent_metanode_info)
               self.__entlist.append(ent)
               print('GnSrchOps: node entity '+node_name+' is mapped ')
            else:
               print("GnSrchOps: NodeDF setup "+node_name+" nodeDF is empty ")
        msg = f" Search Setup is successful"        
        return (0, msg)

    def    gngraph_meta_nodes_edges_setup(self):

        self.get_metanodes_mapped_df()
        self.get_metaedges_mapped_df()

    
    def     gngraph_execute_sqlqry(self, sqlst):
        
        resDF = self.__spark.sql(sqlst)
        ##resDF.show(10)
        ###print(resDF.count())

        resJson = {}
        print('GnSrchOps: executed sql and result: ')
        ##print(resJson)
        return (resDF, resJson)


    def    gngraph_executeqry_getedges_o(self, dnodeDF, sqlst):

         # first map gnedges
         print('GNGraphSrchMain:GetEdges  sql stmt '+sqlst)
         self.get_metaedges_mapped_df()
         self.get_metanodes_mapped_df()
         
         cond=[(self.__gnmetaEdgeDF.gntgtnodeid == dnodeDF.gnnodeid) | (self.__gnmetaEdgeDF.gnsrcnodeid == dnodeDF.gnnodeid)]
         jDF=self.__gnmetaEdgeDF.join(dnodeDF, cond , 'inner')

         print('GNGraphSrchOps: edges for datanode generated')
         ### turn edges result into json 
         edgesJson = jDF.toJSON().map(lambda j: json.loads(j)).collect()
         
         ###jDF.select("gnnodeid", "gnedgeid", \
         ###           "gnsrcnodeid", "gntgtnodeid").show(10)
         
         mcols = [F.col("gnsrcnodeid"), F.col("gntgtnodeid")] 
         
         res = jDF.withColumn("tgtnodes", F.array(mcols))\
                  .select("gnedgeid", "gnnodeid", "tgtnodes")
         ##_union(col("gntgtnodeid"), col("gnsrcnodeid")))
         ##res.show(5)
         
         res2 = res.withColumn("srcnodes", F.array(F.col("gnnodeid")))\
                  .select("*")
         ##res2.show(10)
                  
         res3 = res2.withColumn("filternodes", \
                                F.array_except(F.col("tgtnodes"), \
                                F.col("srcnodes"))).select("*")
         
         ##res3.show()

         #### Transpose column into list
         fDF = res3.select("filternodes").distinct()
         tgtNodeList = res3.select("filternodes").distinct().collect()
         ##for x in tgtNodeList:
         ##    print(x)

         # Iterate over list and get node info from gnmetanodes
         #fDF.printSchema()
         #fDF.show()
         f1DF = fDF.select(F.explode(F.col("filternodes")).alias("fnodes"))
         #f1DF.printSchema()
         #f1DF.show()
         tgtNodeList = f1DF.select("fnodes").distinct().collect()
         #print(tgtNodeList)
         tgt_NodeList=[]
         for row in tgtNodeList:
            ###print(row['fnodes'])
            tgt_NodeList.append(row['fnodes'])

         ### now iterate over list and get gnnode
         print('Preparing the tgtNodeList') 
         nodelist=[]
         for x in tgt_NodeList:
            ###print(x)
            nid = x
            sqlstr="SELECT * from gnmetanodes where gnnodeid="+str(nid)+""
            ##print(sqlstr)
            jDF =  self.__spark.sql(sqlstr)
            ##jDF.printSchema()
            ##j = jDF.toJSON() 
            resJson = jDF.toJSON().map(lambda j: json.loads(j)).collect()
            ##print(resJson[0])
            nodelist.append(resJson[0])

         print('GNGraphSrchOps: tgtNode list enumerated')
         ##print(nodelist)   
         return (edgesJson, nodelist)   

     
    def    gngraph_metarepo_qry_getedges_o(self, rnodeDF, sqlst, derived_nodes_flag):

         # first map gnedges
         print('GnSrchOps: querying for edges and derived nodes flag '+str(derived_nodes_flag))
         self.get_metaedges_mapped_df()
         self.get_metanodes_mapped_df()
         print('GnSrchOps: Enumerating edges and nodes on join ')
         self.__gnmetaEdgeDF.show(4)
         rnodeDF.show(4)
         
         cond=[((self.__gnmetaEdgeDF.gntgtnodeid == rnodeDF.gnnodeid) | (self.__gnmetaEdgeDF.gnsrcnodeid == rnodeDF.gnnodeid)) & (self.__gnmetaEdgeDF.gnedgetype == 'GNMetaNodeEdge')]
         
         jDF=self.__gnmetaEdgeDF.join(rnodeDF, cond , 'inner')
         jDF.show(4)
         jDF.count()
         ##r = jDF.toJSON().map(lambda j: json.loads(j)).collect()
         ##edgesJson = jDF.toJSON().first()
         edgesJson = jDF.toJSON().collect()
         print('GnSrchOps: meta edges for metanodes generated')
         print(edgesJson)
         ### turn edges result into json
         ##edgesJson = jDF.toJSON().map(lambda j: json.loads(j)).collect()
         ##jDF.select("gnnodeid", "gnedgeid", \
         ###           "gnsrcnodeid", "gntgtnodeid").show(10)

         mcols = [F.col("gnsrcnodeid"), F.col("gntgtnodeid")]

         res = jDF.withColumn("tgtnodes", F.array(mcols))\
                  .select("gnedgeid", "gnnodeid", "tgtnodes")
         ##_union(col("gntgtnodeid"), col("gnsrcnodeid")))
         print('GnSrchOps: gnedges filter result 1 ')
         res.show(5)

         res2 = res.withColumn("srcnodes", F.array(F.col("gnnodeid")))\
                  .select("*")
         print('GnSrchOps: gnedges filter result 2')
         res2.show(5)

         res3 = res2.withColumn("filternodes", \
                                F.array_except(F.col("tgtnodes"), \
                                F.col("srcnodes"))).select("*")
         print('GnSrchOps: gnedges filter result 3 ')
         res3.show(5)

         #### Transpose column into list
         fDF = res3.select("filternodes").distinct()
         print('GnSrchOps: filter nodes ')
         fDF.show(5)
         tgtNodeList = res3.select("filternodes").distinct().collect()
         ##tgtNodeList = fDF.collect()
         #for x in tgtNodeList:
         #    print(x)
        
         
         # Iterate over list and get node info from gnmetanodes
         ##fDF.printSchema()
         fDF.show(5)
         f1DF = fDF.select(F.explode(F.col("filternodes")).alias("fnodes"))
         ###f1DF.printSchema()
         f1DF.show(5)
         tgtNodeList = f1DF.select("fnodes").distinct().collect()
         print('GnSrchOps: generating target node list ')
         print(tgtNodeList)
         
         tgt_NodeList=[]
         for row in tgtNodeList:
            ####print(row['fnodes'])
            tgt_NodeList.append(row['fnodes'])

         ### now iterate over list and get gnnode
         print('GnSrchOps: preparing the target node list for search ')
         print(tgt_NodeList)
         nodelist=[]
         nodeid_list = "( "
         i = 0
         for x in tgt_NodeList:
             if (i > 0):
                 nodeid_list += ","
             nodeid_list += ""+str(x)+""
             i = i+1
         nodeid_list += ")"
             
         #for x in tgt_NodeList:
         print(' getting node info for list '+nodeid_list)
         sqlstr="SELECT * from gnmetanodes where gnnodeid in "+nodeid_list+" "
         gn_log('GnGraphSearchOps: executing sql '+sqlstr)
         jDF =  self.__spark.sql(sqlstr)
         ##jDF.printSchema()
         #j = jDF.toJSON()
         #print(' Node info '+str(x))
         #print(j)
         resJson = jDF.toJSON().map(lambda j: json.loads(j)).collect()
         #resJson = jDF.toJSON().first()
         print(' Node info '+str(x))
         print(resJson)
         #nodelist.append(resJson[0])
         nodelist = resJson
         print('GNGraphSrchOps: tgtNode list enumerated')
         ####print(nodelist)
         
         return (edgesJson, nodelist)

    """
        Get meta edges and the derived nodes based one edges
          The derived nodes are present in the edge but in the source list for example: Customer has edge to product id so when customer id is queried, then product info also shows up in sarch result
    """
    
    def    gngraph_metarepo_qry_getedges(self, rnodeDF, sqlst, derived_nodes_flag):

        # first map gnedges
        print('GnSrchOps:metanodes  querying for edges and derived nodes flag '+str(derived_nodes_flag))
        self.get_metaedges_mapped_df()
        self.get_metanodes_mapped_df()
        print('GnSrchOps: Enumerating edges and nodes on join ')
        self.__gnmetaEdgeDF.show(4)
        rnodeDF.show(4)
         
        cond=[((self.__gnmetaEdgeDF.gntgtnodeid == rnodeDF.gnnodeid) | (self.__gnmetaEdgeDF.gnsrcnodeid == rnodeDF.gnnodeid)) & (self.__gnmetaEdgeDF.gnedgetype == 'GNMetaNodeEdge')]
         
        jDF=self.__gnmetaEdgeDF.join(rnodeDF, cond , 'inner')
         
        jDF.show(4)
        e1DF = jDF.select("gnedgeid", "gnedgename", "gnedgetype", "gnsrcnodeid", "gntgtnodeid")
         
        eDF = e1DF.dropDuplicates(['gnedgeid']).sort('gnedgeid')
        ecount = eDF.count()
        print('GnSrchOps: showing unique edges #nodes '+str(ecount))
        eDF.show(5)

        # Bring all nodes (src and tgt) into a datframe
        # filter whats in the source node list (derived nodes) and
        # get node info for derived nodes
         
        mcols = [F.col("gnsrcnodeid"), F.col("gntgtnodeid")]

        res = eDF.withColumn("edgenodes", F.array(mcols))\
                  .select("edgenodes")
        ##_union(col("gntgtnodeid"), col("gnsrcnodeid")))
        print('GnSrchOps: gnedges filter result 1 ')
        res.show(5)

        f1DF = res.select(F.explode(F.col("edgenodes")).alias("gnnodeid"))
        f1count = f1DF.count()
        print('GnSrchOps: Filter nodes exploded #nodes '+str(f1count))        
        f1DF.show(10)
         
        print('GnSrchOps: Filtered nodes and remove duplicates ')         
        f2DF = f1DF.select("gnnodeid").distinct().sort("gnnodeid")
        f2count = f2DF.count()
        print('GnSrchOps: Filter nodes and distict #nodes '+str(f2count))
        f2DF.show(10)

        ### filter enodes from source nodes aka left antijoin (rnodeDF - f1DF)
        derivedNodeDF = rnodeDF.select("gnnodeid").join(f2DF, on=['gnnodeid'], how='left_anti').distinct().orderBy('gnnodeid')

        print('GnSrchOps: Derived nodes ')
        
        nderivedNodes = derivedNodeDF.count()
        print('GnSrchOps: derived datanodes #of nodes '+str(nderivedNodes))
         
        dnJson = {}
        dnDF = None
        if (nderivedNodes > 0):
            derivedNodeDF.show(10)
            deriveNodeList = derivedNodeDF.collect()
            derived_NodeList=[]
            for row in derivedNodeList:
               ####print(row['fnodes'])
               derived_NodeList.append(row['gnnodeid'])
            ### now iterate over list and get gnnode
            print('GnSrchOps: Node info for derived nodes ')
            print(derived_NodeList)
            nodelist=[]
            nodeid_list = "( "
            i = 0
            for x in derived_NodeList:
               if (i > 0):
                  nodeid_list += ","
               nodeid_list += ""+str(x)+""
               i = i+1
            nodeid_list += ")"
            print('GnSrchOps: Getting node info for list '+nodeid_list)
            sqlstr="SELECT * from gnmetanodes where gnnodeid in "+nodeid_list+" "
            gn_log('GnGraphSearchOps: executing sql '+sqlstr)
            dnDF =  self.__spark.sql(sqlstr)
            #resJson = jDF.toJSON().map(lambda j: json.loads(j)).collect()
            ###dnJson = dnDF.toJSON().collect()
            print('GnSrchOps: Derived nodes json ')
            #print(dnJson)
            print('GnSrchOps: Derived nodes enumerated ')
            ####print(nodelist)
        print('GnSrchOps: Completed gnedges fetch ')
        ##edgesJson = eDF.toJSON().collect()            
        return (eDF, dnDF)


    def    gngraph_datarepo_qry_getedges(self, dnodeDF, sqlst):

        # first map gnedges
        print('GnSrchOps:datanodes  querying for edges and derived nodes flages ')
        self.get_metaedges_mapped_df()
        self.get_metanodes_mapped_df()
         
        print('GnSrchOps: Enumerating edges for datanodes on join ')
        cond=[((self.__gnmetaEdgeDF.gntgtnodeid == dnodeDF.gnnodeid) | (self.__gnmetaEdgeDF.gnsrcnodeid == dnodeDF.gnnodeid)) & (self.__gnmetaEdgeDF.gnedgetype == 'GNDataNodeEdge')]
         
        jDF=self.__gnmetaEdgeDF.join(dnodeDF, cond , 'inner')
        jDF.show(4)
         
        e1DF = jDF.select("gnedgeid", "gnedgename", "gnedgetype", "gnsrcnodeid", "gntgtnodeid")
         
        eDF = e1DF.dropDuplicates(['gnedgeid']).sort('gnedgeid')
        ecount = eDF.count()
        print('GnSrchOps: showing unique edges #nodes '+str(ecount))
        eDF.show(5)
         
        mcols = [F.col("gnsrcnodeid"), F.col("gntgtnodeid")]          
        res = eDF.withColumn("edgenodes", F.array(mcols))\
                  .select("edgenodes")
        print('GnSrchOps: gnedges filter result 1 ')
        res.show(5)

        f1DF = res.select(F.explode(F.col("edgenodes")).alias("gnnodeid"))
        f1count = f1DF.count()
        print('GnSrchOps: Filter datanodes exploded #nodes '+str(f1count))
        f1DF.show(10)
         
        print('GnSrchOps: Filtered datanodes and remove duplicates ')
        f2DF = f1DF.select("gnnodeid").distinct().sort("gnnodeid")
        f2count = f2DF.count()
        print('GnSrchOps: Filter nodes and distict #nodes '+str(f2count))
        f2DF.show(10)

        derivedNodeDF = dnodeDF.select("gnnodeid").join(f2DF, on=['gnnodeid'], how='left_anti').distinct().orderBy('gnnodeid')
         
        print('GnSrchOps: Enumerating derived datanodes ')
        
        nderivedNodes = derivedNodeDF.count()
        print('GnSrchOps: derived datanodes #of nodes '+str(nderivedNodes))
        dnJson = {}
        dnDF = None
        if (nderivedNodes > 0):
            derivedNodeDF.show(10)
            deriveNodeList = derivedNodeDF.collect()
            derived_NodeList=[]
            for row in derivedNodeList:
               ####print(row['fnodes'])
               derived_NodeList.append(row['gnnodeid'])
            ### now iterate over list and get gnnode
            print('GnSrchOps: Node info for derived datanodes ')
            print(derived_NodeList)
            nodelist=[]
            nodeid_list = "( "
            i = 0
            for x in derived_NodeList:
               if (i > 0):
                  nodeid_list += ","
               nodeid_list += ""+str(x)+""
               i = i+1
            nodeid_list += ")"
            print('GnSrchOps: Getting node info for list '+nodeid_list)
            sqlstr="SELECT * from gnmetanodes where gnnodeid in "+nodeid_list+" "
            gn_log('GnGraphSearchOps: executing sql '+sqlstr)
            dnDF =  self.__spark.sql(sqlstr)
            #resJson = jDF.toJSON().map(lambda j: json.loads(j)).collect()
            ###dnJson = dnDF.toJSON().collect()
            dnCount = dnDF.count()
            print('GnSrchOps: Derived datanodes enumerated #nodes '+str(dnCount))            
        ####print(nodelist)
        print('GnSrchOps: Completed datanodes gnedges fetch ')            
        return (eDF, dnDF)




    
def       gngraph_init(rootDir):

    app_name = "gngraph"
    gngraph_cls = gnGraphSrchDBOps(rootDir)

    console.log(' Gngraph Init no spark Context')
    ### Set spark session
    ##spark = SparkSession.builder.appName(app_name).getOrCreate()

    return gngraph_cls




def     gngrph_srch_get_entlist_obsolete(sqlst):

    print('gnsrch_process_select_conevert: processing sqlstring ' + sqlst)

    jsql = parse(sqlst)
    selstr = "select"
    retval = 0
    cql = ''
    if (selstr in jsql):
        # Select statement

        print('gnsrch_process_select_convert_cypher: Processing select:')
        attrlist = jsql[selstr]
        entlist = jsql["from"]

        print('gnsrch_process_select_convert_cypher: attrlist:' + attrlist)
        print('gnsrch_process_select_convert_cypher: entitylist:' + entlist)
        #if (attrlist == "*"):
            # Ex: Select * from Customer
        #    cql += "MATCH (" + str(entlist) + " "
        #    cql += "{metanode:'" + str(entlist) + "'}) "
        #    cql += " return " + str(entlist) + " LIMIT 10 ;"

        #    if (verbose > 4):
        #        print('gnsrch_process_select_convert_cypher: cqlqry :' + cql)   
        #    return cql
        nodes_list = []
        if (isinstance(entlist, list)):
            nodes_list = entlist
        else:
            # for single node entry, entlist is a string
            nodes_list.append(entlist)

        
        return nodes_list



def        gngrph_search_init(gnp_spark, gndata_folder, gngraph_creds_folder, accessmode):

        print('GnSrchOps: Init SearchOps using spark session ')
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
        ###entlist = gngrph_srch_get_entlist(sqlst)                                         
        gnsrch_ops = GNGraphSearchOps(gndata_folder, accessmode, fargs, gdbargs, gnp_spark)
        ## Map metanodes and edges
        gnsrch_ops.get_metanodes_mapped_df()
        gnsrch_ops.get_metaedges_mapped_df()
        if (gnsrch_ops.srch_init_data_status() == 1):
            print('GnSrchOps: ERROR srchops init failed  ')
            gnsrch_ops = ''
            return gnsrch_ops

        
        print('GnSrchOps: gngraph searchOps init COMPLETE ')
        return gnsrch_ops

    
def        gngrph_srch_datarepo_qry_execute(gnsrch_ops, sqlst, nodesonly):
                
    
    (resNodeDF, nodesjson) = gnsrch_ops.gngraph_execute_sqlqry(sqlst)
    #resNodeDF.show(10)
    nodeCount = resNodeDF.count()
    print('GnSrchOps: datanodes fetched #nodes '+str(nodeCount))
    ejson = {}
    njson = {}
    if (nodeCount > 0):    
        resNodeDF.show(5)

        #nDF = resNodeDF.select(col("gnnodeid").alias("id"), \
        #                       col("gnnodetype").alias("nodetype"), \
        #                       col("gnlabel").alias("nodename"))
        nDF = resNodeDF \
                 .withColumnRenamed("gnnodeid", "id") \
                 .withColumnRenamed("gnnodetype", "nodetype") \
                 .withColumnRenamed("gnlabel", "nodename")
        
        
        njson = nDF.toJSON().collect()

        if (nodesonly == 0):  
            (eDF, dnDF) = gnsrch_ops.gngraph_datarepo_qry_getedges(resNodeDF, sqlst)
            
            nEdges = eDF.count()
            if (dnDF is not None):
                nDNodes = dnDF.count()
            else:
                nDNodes = 0
                
            if (nEdges > 0):
                eResDF = eDF.select(col("gnedgeid").alias("id"), \
                              col("gnedgetype").alias("type"), \
                              col("gnsrcnodeid").alias("source"), \
                              col("gntgtnodeid").alias("target"))
                ejson = eResDF.toJSON().collect()

                if (nDNodes > 0):
                      dnDF1 = dnDF.select(col("gnnodeid").alias("id"), \
                                  col("gnnodetype").alias("nodetype"), \
                                  col("gnnodename").alias("nodename"))
                      
                      # Combine the derived nodes to source nodes
                      rDF = nDF.unionByName(dnDF1, allowMissingColumns=True)
                      njson = rDF.toJSON().collect()
                      print('GnSrchOps:  Nodes Json ')
                      #print(njson)
                      print('GnSrchOps:  Edges Json ')
                      #print(ejson) 
                      
    print('GnSrchOps: datanodes and edges qry complete SUCCESS')
    return (njson, ejson)

#######
def          gngrph_srch_metarepo_qry_execute(gnsrch_ops, gnp_spark, sqlst, nodesonly):
    
        njson = {}
        ejson = {}
        gnsrch_ops.gngraph_meta_nodes_edges_setup()

        if (gnsrch_ops.srch_init_meta_status() == 0):
            gn_log('GnSrchOps: Meta data initialized is not completed ')
            njson = {}
            edgesjson = {}
            return (njson, edgesjson)
            
        (resNodeDF, nodesjson) = gnsrch_ops.gngraph_execute_sqlqry(sqlst)

        print('GnSrchOps: metanodes  fetched ')
        print('GnSrchOps: sql st:'+sqlst)
        ##print(nodesjson)
        nnodes = resNodeDF.count()
        print('GnSrchOps: metanodes for search returned #nodes '+str(nnodes))
        if (nnodes > 0):        
            resNodeDF.show(5)
            ## Prepare njson output
            nDF = resNodeDF.select(col("gnnodeid").alias("id"), \
                                  col("gnnodetype").alias("nodetype"), \
                                  col("gnnodename").alias("nodename"))
            njson = nDF.toJSON().collect()
            
            
            if (nodesonly == 0): 
               ### Need to derive edges and derived nodes 
               (eDF, dnDF) = gnsrch_ops.gngraph_metarepo_qry_getedges(resNodeDF, sqlst, 0)

               nEdges = eDF.count()
               if (dnDF is not None):
                 nDNodes = dnDF.count()
               else:
                 nDNodes = 0

               if (nEdges > 0):
                  eResDF = eDF.select(col("gnedgeid").alias("id"), \
                              col("gnedgetype").alias("type"), \
                              col("gnsrcnodeid").alias("source"), \
                              col("gntgtnodeid").alias("target"))
                  ejson = eResDF.toJSON().collect()

                  if (nDNodes > 0):

                      dnDF1 = dnDF.select(col("gnnodeid").alias("id"), \
                                  col("gnnodetype").alias("nodetype"), \
                                  col("gnnodename").alias("nodename"))

                      # Combine the derived nodes to source nodes
                      rDF = nDF.unionByName(dnDF1, allowMissingColumns=True)

                      njson = rDF.toJSON().collect()
                      print('GnSrchOps:  Nodes Json ')
                      print(njson)
                      print('GnSrchOps:  Edges Json ')
                      print(ejson)        

        print('GnSrchOps: Meta edges and derived nodes enumerated ')           
        return (njson, ejson)
        


def         gngrph_srch_datarepo_qry_fetch_api(gnsrch_ops, gnp_spark, sqlst, nodesonly):

    
    print('GnSrchOps: datanodes qry fetch ')

    (retval, msg) = gnsrch_ops.gngraph_search_setup(sqlst)


    if (retval < 0):
        print('GnSrchOps: Search failed with msg '+msg)           
        rJ = {}
        rJ["nodes"] = []
        rJ["edges"] = []
        rJ["nodelen"] = 0
        rJ["edgelen"] = 0
        rJ["status"] = "ERROR"
        rJ["errmsg"] = msg
        return(rJ)
    
    (nJson, eJson) = gngrph_srch_datarepo_qry_execute(gnsrch_ops, sqlst, nodesonly)
    print('GnSrchOps: datanodes qry executed ')
    print(nJson)
    print('GnSrchOps: edges enumerated ')
    print(eJson)
    rJ = {}
    rJ["nodes"] = nJson
    rJ["nodelen"] = len(rJ["nodes"])
    rJ["edges"] = eJson
    rJ["edgelen"] = len(rJ["edges"])
    rJ["status"] = "SUCCESS"

    return(rJ)


def        gngrph_srch_metarepo_qry_fetch_nodes_api(gnsrch_ops, gnp_spark, srchfilter):
    
    sqlst = "select * from gnmetanodes WHERE gnnodetype='GNMetaNode' OR gnnodetype='GNMetaNodeAttr'"
    

    if (gnsrch_ops.srch_init_meta_status() == 0):
        rj={}
        rj["nodes"]=[]
        rj["edges"]=[]
        rJ["nodelen"] = 0
        rJ["edgelen"] = 0
        rJ["status"] = "ERROR"
        return rj


    nodesonly = 1
    (mnJson, meJson) = gngrph_srch_metarepo_qry_execute(gnsrch_ops, gnp_spark, sqlst, nodesonly)
        
    rJ = {}

    rJ["nodes"] = mnJson
    rJ["edges"] = meJson
    rJ["nodelen"] = len(rJ["nodes"])
    rJ["edgelen"] = len(rJ["edges"])
    rJ["status"] = "SUCCESS"
    print('GnSrchOps: Meta nodes fetched')
    #print(rJ)
     
    return(rJ)


def        gngrph_srch_metarepo_qry_fetch_api(gnsrch_ops, gnp_spark, srchfilter):
    
    sqlst = "select * from gnmetanodes WHERE gnnodetype='GNMetaNode' OR gnnodetype='GNMetaNodeAttr'"
    

    if (gnsrch_ops.srch_init_meta_status() == 0):
        rj={}
        rj["nodes"]=[]
        rj["edges"]=[]
        rJ["nodelen"] = 0
        rJ["edgelen"] = 0
        rJ["status"] = "ERROR"
        return rj


    nodesonly = 0
    (mnJson, meJson) = gngrph_srch_metarepo_qry_execute(gnsrch_ops, gnp_spark, sqlst, nodesonly)
        
    rJ = {}

    rJ["nodes"] = mnJson
    rJ["edges"] = meJson
    rJ["nodelen"] = len(rJ["nodes"])
    rJ["edgelen"] = len(rJ["edges"])
    rJ["status"] = "SUCCESS"
    print('GnSrchOps: Meta nodes and edges and derived nodes fetched')
    #print(rJ)
     
    return(rJ)

    

def        test_datarepo_qry_fn():

    print("GnSrchOps: Test gnsearch datarepo qry ")    
    app_name="gngraph"
    gndata_folder=pparentDir+"/gndata"
    gngraph_creds_folder = pparentDir+"/creds/gngraph"
        
    sqlst = "SELECT * from Customer LIMIT 10000"                
    
    ### Set spark session
    gnp_spark = SparkSession.builder.appName(app_name).getOrCreate()
    nodesonly = 0
    accessmode={'sfmode': 1, 'dbmode':0 }
    gngrph_cls = gngrph_search_init(gnp_spark, gndata_folder, gngraph_creds_folder, accessmode)

    
    nodesonly = 1
    rJ = gngrph_srch_datarepo_qry_fetch_api(gngrph_cls, gnp_spark, sqlst,  nodesonly)

    if (rJ["status"] == "ERROR"):
        print('GnSrchOps: Testing search query failed ')
        print('GnSrchOps: Err msg: '+rJ["errmsg"])
        return
    
    
    rfile="dnodes.json" 
    with open(rfile, 'w') as fp:
            json.dump(rJ["nodes"], fp)

    efile="dedges.json"
    with open(efile, "w") as fp:
            json.dump(rJ["edges"], fp)

            
def     gnspk_process_request_thrfn(gngrph_cls, gnp_spark, req):


    if (req["cmd"] == "metasearch"):
        srchfilter = req["srchstring"]
        if (srchfilter == "null"):
            srchfilter = ""
        rJ = gngrph_srch_metarepo_qry_fetch_api(gngrph_cls, gnp_spark, srchfilter)
        resp = {}
        resp["cmd"] = req["cmd"]
        resp["status"] = "SUCCESS"
        resp["data"] = rJ
        return resp
     ## datasearch
    if (req["cmd"] == "datasearch"):
        sqlst = req["srchstring"]
        
        if (sqlst == "null"):
            sqlst = ""
            resp={}
            resp["cmd"] = req["cmd"]
            resp["status"] = "ERROR"
            resp["data"] = []
            resp["errmsg"] = "No Search string"
            return resp
        
        nodesonly = req["nodeonly"]

        rJ = gngrph_srch_datarepo_qry_fetch_api(gngrph_cls, gnp_spark, sqlst, nodesonly)
        resp = {}
        resp["cmd"] = req["cmd"]
        resp["status"] = "SUCCESS"
        resp["data"] = rJ
        return resp

            
def     gnspk_thread_main(gnRootDir, accessmode, req_q, resp_q):
    
    print('GnSrchOps: Starting Spark Session thread ')
    app_name = "gngraph"
    gndata_folder = gnRootDir+"/gndata"
    gngraph_creds_folder = gnRootDir+"/creds/gngraph"

    gn_log('GnSrchOpsThr: Initializing Spark Session thread ' )

    conf = SparkConf()
    conf.set('spark.executor.memory', '4g')
    conf.set('spark.driver.memory', '4g')
    
    gnp_spark = SparkSession.builder.appName(app_name).getOrCreate()
    
    #gnp_spark.sparkContext.setLogLevel("INFO")
    
    gngrph_cls = gngrph_search_init(gnp_spark, gndata_folder, gngraph_creds_folder, accessmode)
  
    ### Initialized Spark Session and now wait for some task
    while True:
        print('GnSrchOps: Thread waiting for request ')
        req = req_q.get()


        if (req is None):
            print('Empty request returned ')
            req_q.task_done()
            return
        else:
            resp = gnspk_process_request_thrfn(gngrph_cls, gnp_spark, req)
            # put the response on output queue
            resp_q.put(resp)
            
        time.sleep(4)
        print('GnSrchOps: Processing of message done')
        
    


def       gnp_spark_thread_setup(gnRootDir, accessmode):

    request_que = Queue()
    response_que = Queue()

    ## Start Gnspark thread
    gn_spk_thr = Thread(target=gnspk_thread_main, args=(gnRootDir, accessmode, request_que, response_que,))
    
    ## Set thread as daemon
    gn_spk_thr.setDaemon(True)
    gn_spk_thr.start()

    gnspk_thr_config = {}
    gnspk_thr_config["request_queue"] = request_que
    gnspk_thr_config["response_queue"] = response_que
    gnspk_thr_config["spkthr"] = gn_spk_thr
    
    return gnspk_thr_config

def       gnp_spark_thread_join(gnspk_thr_config):
    print(' Joining for the thread ')
    gnspk_thr_config["spkthr"].join()

    
def       gnp_spark_thread_send_receive_task(gnspk_thr_config, tskmsg):
   
    # Send the task on request queue
    gnspk_thr_config["request_queue"].put(tskmsg)
    time.sleep(1)
    
    #Now wait for request
    resp = gnspk_thr_config["response_queue"].get()

    return resp


def     gnp_spark_app_server_socket(gnRootDir):
    
    print("Starting the gnspark thread application")
    app_name="gngraph"

    ##gndata_folder=pparentDir+"/gndata"
    ##gngraph_creds_folder = pparentDir+"/creds/gngraph"
    accessmode={'sfmode': 1, 'dbmode':0 }
    gnspk_thr_cfg = gnp_spark_thread_setup(gnRootDir, accessmode)

    print(" Starting socket server...")
    
    SERVER_HOST = "0.0.0.0"
    SERVER_PORT = 4141
    BUFFER_SIZE = 4096
    SEPARATOR = ","
    
    s = socket.socket()
    s.bind((SERVER_HOST, SERVER_PORT))
    s.listen(10)
    
    print(f"[*] Listening as {SERVER_HOST}:{SERVER_PORT}")
    print("Waiting for the client to connect... ")

    while True:
        
        client_sock, address = s.accept()
    
        print(f"[+] {address} is connected.")
        received = client_sock.recv(BUFFER_SIZE).decode()
        print('GnSrchOps: received command ')
        print(received)
        
        ##(cmd, args, nodeonly) = received.split(SEPARATOR)
        cmdJ = json.loads(received)
        print(cmdJ)

        cmd = cmdJ["cmd"]
        args = cmdJ["args"]
        nodeonly = cmdJ["nodeonly"]
        
        print('cmd '+cmdJ["cmd"])
        print(' args '+ cmdJ["args"])
        print(' nodeonly '+str(nodeonly))
        
        tskmsg = {}
        tskmsg["cmd"] = cmd
        tskmsg["srchstring"] = args
        tskmsg["nodeonly"] = nodeonly
        
        resp = gnp_spark_thread_send_receive_task(gnspk_thr_cfg, tskmsg)
        resp_str = json.dumps(resp)
        ###progress = tqdm.tqdm(range(filesize), f"Receiving {filename}", unit="B", unit_scale=True, unit_divisor=1024)
        # Send message 
        client_sock.sendall(resp_str.encode())
        print(f" sent the response back ")
    
        #with open(filename, "wb") as f:
        #    while True:
        #        bytes_read = client_socket.recv(BUFFER_SIZE)
        #        if not bytes_read:
        #            break
        #        f.write(bytes_read)
        #        progress.update(len(bytes_read))
            
        client_sock.close()
     
    s.close()        
    # join the thread
    gnp_spark_thread_join(gnspk_thr_cfg)
    


        
def     test_spkthread_fns(gnRootDir):

    print("Testing the metarepo file")
    app_name="gngraph"

    ##gndata_folder=pparentDir+"/gndata"
    ##gngraph_creds_folder = pparentDir+"/creds/gngraph"
    accessmode={'sfmode': 1, 'dbmode':0 }
    gnspk_thr_cfg = gnp_spark_thread_setup(gnRootDir, accessmode)

    ### Send the request and wait for response
    tskmsg = {}
    tskmsg["cmd"] = "metasearch"
    tskmsg["srchstring"] =  ""

    #resp = gnp_spark_thread_send_receive_task(gnspk_thr_cfg, tskmsg)

    #print('GnSrchOps: response from thread ')
    #print(resp)
    time.sleep(1)
    # Send data request
    tskmsg = {}
    tskmsg["cmd"] = "datasearch"
    tskmsg["srchstring"] = "SELECT * from Customer LIMIT 10000"     

    resp = gnp_spark_thread_send_receive_task(gnspk_thr_cfg, tskmsg)
      
    # join the thread
    gnp_spark_thread_join(gnspk_thr_cfg)
    
    
def     test_metarepo_qry_fn():

    print("Testing the metarepo file")
    app_name="gngraph"

    gndata_folder=pparentDir+"/gndata"
    gngraph_creds_folder = pparentDir+"/creds/gngraph"
        
    sqlst = "select * from gnmetanodes WHERE gnnodetype='GNMetaNode' OR gnnodetype='GNMetaNodeAttr'"
    ### Set spark session

    gnp_spark = SparkSession.builder.appName(app_name).getOrCreate()
    nodesonly = 0
    accessmode={'sfmode': 1, 'dbmode':0 }
    gngrph_cls = gngrph_search_init(gnp_spark, gndata_folder, gngraph_creds_folder, accessmode)
    
    #(nJSON, eJSON) = gngrp_srch_qry_api(gngrph_cls, sqlst,  nodesonly)
    (nJSON, eJSON) = gngrph_srch_metarepo_qry_api(gngrph_cls, gnp_spark, sqlst, 0)
    
    rfile="metanodes.json" 
    with open(rfile, 'w') as fp:
            json.dump(nJSON, fp)

    efile="metaedges.json"
    with open(efile, "w") as fp:
            json.dump(eJSON, fp)


            
            
if __name__ == "__main__":
     
 print(' Parent Dir ' + pparentDir)
 #test_metarepo_qry_fn()
 #test_datarepo_qry_fn()
 #test_spkthread_fns(pparentDir)
 gnp_spark_app_server_socket(pparentDir)
