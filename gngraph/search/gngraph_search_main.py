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
from moz_sql_parser import parse
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

from gngraph.config.gngraph_config import GNGraphConfig
from gngraph.gngraph_dbops.gngraph_pgresdbops_srch import GNGraphSrchPgresDBOps
from gngraph.gngraph_dbops.gngraph_staticfileops_srch import GNGraphSrchStaticFileOps
from gngraph.search.gngraph_sqlparser import GNGraphSqlParserOps
from gn_config import gn_log, gn_log_err


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
        
        self.gngraph_config_init(gngrp_datadir, accessmode, fargs, dbargs)
        print("GNGrphSrchMain:  Search Init for entlist ")
        ###print(self.__entlist)
        self.__init_data = 0

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

        if (self.__gncfg["staticfiles"] == 1):
           metanode_jobj = self.__gngrp_sfops.get_metanode_info(ent_name, self.__spark)

        if (self.__gncfg["gdbflag"] == 1):
           metanode_jobj = self.__gngrp_dbops.get_metanode_info(ent_name, self.__spark)

        return metanode_jobj   

    
    def    get_datanode_mapped_df(self, node_name, bizdomain):
 
         if (self.__gncfg["staticfiles"] == 1):
            dnodeDF = self.__gngrp_sfops.get_datanode_mapped_df(node_name, bizdomain, self.__spark)
         elif (self.__gncfg["gdbflag"] == 1):
            dnodeDF = self.__gngrp_dbops.get_datanode_mapped_df(node_name, bizdomain, self.__spark)
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

        self.__gn_srch_sql = sqlstmt  
        gn_log("GNGrphSrchOps: Parsing sql st "+self.__gn_srch_sql)  
        self.__gn_ssql_parsed = GNGraphSqlParserOps(self.__gn_srch_sql)
        
        #self.__entlist
        t = self.__gn_ssql_parsed.get_entlist()
        aEntList = []
        for x in t:
            if x not in self.__entlist:
               aEntList.append(x)
               
        
        self.__gn_ssql_parsed_where_str = self.__gn_ssql_parsed.get_where_str()
        ###self.__gngrp_dnDFList = []
        
        
        ###entlist is list of nodesname that need to be mapped
        for ent in aEntList:            
            entD = {}                            
            ent_metanode_info =  self.get_metanode_info(ent)
            jprop = json.loads(ent_metanode_info["gnnodeprop"])
            node_name = ent_metanode_info["gnnodename"]
            bizdomain = jprop["bizdomain"]

            gn_log("GNGrphSrchOps:  setup api nodename "+node_name+" bizdomain:"+bizdomain)
            entnodeDF = self.get_datanode_mapped_df(node_name, bizdomain)

            if (entnodeDF is not None):
               ent_metanode_info["df"] = entnodeDF
               self.__gngrp_dnDFList.append(ent_metanode_info)
               self.__entlist.append(ent)
            else:
               gn_log("GNGrphSrchOps: NodeDF setup "+node_name+" nodeDF is empty ")

               
        return

    def    gngraph_meta_nodes_edges_setup(self):

        self.get_metanodes_mapped_df()
        self.get_metaedges_mapped_df()
        
        
    
    def     gngraph_execute_sqlqry(self, sqlst):        
        resDF = self.__spark.sql(sqlst)
        ##resDF.show(10)
        ###print(resDF.count())
        ##resJson = resDF.toDF().toJSON()
        #resJson = resDF.toJSON().first()
        resJson = resDF.toJSON().map(lambda j: json.loads(j)).collect()
        ##print(results)
        print('gngraph_execut_sql: ')
        ##print(resJson)
        return (resDF, resJson)


    def    gngraph_executeqry_getedges(self, dnodeDF, sqlst):

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
         ##res.show(10)
         
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

     
    def    gngraph_metarepo_qry_getedges(self, rnodeDF, sqlst):

         # first map gnedges
         print('GNGraphSrchMain: sql stmt '+sqlst)
         self.get_metaedges_mapped_df()
         self.get_metanodes_mapped_df()
         cond=[((self.__gnmetaEdgeDF.gntgtnodeid == rnodeDF.gnnodeid) | (self.__gnmetaEdgeDF.gnsrcnodeid == rnodeDF.gnnodeid)) & (self.__gnmetaEdgeDF.gnedgetype == 'GNMetaNodeEdge')]
         jDF=self.__gnmetaEdgeDF.join(rnodeDF, cond , 'inner')

         print('GNGraphSrchOps: edges for datanode generated')
         ### turn edges result into json
         edgesJson = jDF.toJSON().map(lambda j: json.loads(j)).collect()
         ##jDF.select("gnnodeid", "gnedgeid", \
         ###           "gnsrcnodeid", "gntgtnodeid").show(10)

         mcols = [F.col("gnsrcnodeid"), F.col("gntgtnodeid")]

         res = jDF.withColumn("tgtnodes", F.array(mcols))\
                  .select("gnedgeid", "gnnodeid", "tgtnodes")
         ##_union(col("gntgtnodeid"), col("gnsrcnodeid")))
         ####res.show(10)

         res2 = res.withColumn("srcnodes", F.array(F.col("gnnodeid")))\
                  .select("*")
         ###res2.show(10)

         res3 = res2.withColumn("filternodes", \
                                F.array_except(F.col("tgtnodes"), \
                                F.col("srcnodes"))).select("*")
         ###res3.show()

         #### Transpose column into list
         fDF = res3.select("filternodes").distinct()
         tgtNodeList = res3.select("filternodes").distinct().collect()
         ####for x in tgtNodeList:
         ####    print(x)

         # Iterate over list and get node info from gnmetanodes
         ##fDF.printSchema()
         ##fDF.show()
         f1DF = fDF.select(F.explode(F.col("filternodes")).alias("fnodes"))
         ###f1DF.printSchema()
         ###f1DF.show()
         tgtNodeList = f1DF.select("fnodes").distinct().collect()
         #print(tgtNodeList)
         tgt_NodeList=[]
         for row in tgtNodeList:
            ####print(row['fnodes'])
            tgt_NodeList.append(row['fnodes'])

         ### now iterate over list and get gnnode
         print('Preparing the tgtNodeList')
         nodelist=[]
         for x in tgt_NodeList:
            ###print(x)
            nid = x
            sqlstr="SELECT * from gnmetanodes where gnnodeid="+str(nid)+""
            ####print(sqlstr)
            jDF =  self.__spark.sql(sqlstr)
            ##jDF.printSchema()
            ##j = jDF.toJSON()
            resJson = jDF.toJSON().map(lambda j: json.loads(j)).collect()
            ##print(resJson[0])
            nodelist.append(resJson[0])

         print('GNGraphSrchOps: tgtNode list enumerated')
         ####print(nodelist)
         return (edgesJson, nodelist)




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
        if (gnsrch_ops.srch_init_data_status() == 1):
            gn_log_err('GNGrphSrchOps: SearchOps Init failed ')
            gnsrch_ops = ''
            return gnsrch_ops

        
        gn_log('GNGrphSrchOps: GNGraph SearchOps init COMPLETE ')
        return gnsrch_ops

    
def        gngrp_srch_qry_api(gnsrch_ops, sqlst, nodesonly):
                
    gnsrch_ops.gngraph_search_setup_api()
    
    (resNodeDF, nodesjson) = gnsrch_ops.gngraph_execute_sqlqry(sqlst)
    gn_log('GNGrphSrchOps: Executed SQL Qry  '+sqlst)
    gn_log('GNGrphSrchOps: fetched data nodes. getting edges ')
    ###resNodeDF.show(10)       
    edgesjson = ""
    if (nodesonly == 0):  
        (edgesjson,derived_nodesjson) = gnsrch_ops.gngraph_executeqry_getedges(resNodeDF, sqlst)    
        gn_log('GNGrphSrchOps: edges fetched and getting derived nodes')
        njson = nodesjson+derived_nodesjson
    else:
        njson = nodesjson
        edgesjson={}
    return (njson, edgesjson)

#######
def          gngrph_srch_metarepo_qry_api(gnsrch_ops, gnp_spark, sqlst, nodesonly):

        #gdb_creds_filepath=gngraph_creds_folder+"/gngraph_pgres_dbcreds.json"
        #fileargs = {}
        #gdbargs = {}
        #gdbargs["gdb"] = "pgres"
        #gdbargs["gdbflag"] = 1
        #gdbargs["gdbcredsfpath"] = gdb_creds_filepath
        #gdbargs["gnmetaDB"] = "gngraph_db"
        #gdbargs["gndataDB"] = "gngraph_db"
        #gdbargs["staticfiles"] = 1
        #gdbargs["staticfpath"] = gndata_folder+"/uploads";
        #gdbargs["gndatafolder"] = gndata_folder
        #fargs = {}
        #fargs["gngraphfolder"] = gndata_folder+"/gngraph"
        #fargs["gnmetanodesfname"] = "gnmeanodes.json"
        #fargs["gnmetaedgesfname"] = "gnmetaedges.json"
        ##accessmode="pgres"
        ###entlist = gngrph_srch_get_entlist(sqlst)
        ##gnsrch_ops = GNgraphSearchOps(sqlst, gndata_folder, accessmode, fargs, gdbargs, gnp_spark)
        ###where_str = self.__gn_ssql_parsed_where_str
        #####msql_st = "SELECT * from gnmetanodes  WHERE gnnodetype='GNMetaNode'"        
        ###gnsrch_ops.gngraph_search_metarepo_setup_api()

        gnsrch_ops.gngraph_meta_nodes_edges_setup()

        if (gnsrch_ops.srch_init_meta_status() == 0):
            gn_log('GNGrphSrchOps: Meta data initialized is not completed ')
            njson = {}
            edgesjson = {}
            return (njson, edgesjson)
            
        (resNodeDF, nodesjson) = gnsrch_ops.gngraph_execute_sqlqry(sqlst)
        gn_log('GNGrphSrchOps: datanodes fetched ')
        gn_log('GNGrphSrchOps: sql st:'+sqlst)
        ##print(nodesjson)
        gn_log('GNGrphSrchOps: Search meta nodes complete. get edges ')
        ####resNodeDF.show(10)
        edgesjson = ""
        (edgesjson, derived_nodesjson) = gnsrch_ops.gngraph_metarepo_qry_getedges(resNodeDF, sqlst)
        njson = nodesjson+derived_nodesjson
        
        return (njson, edgesjson)



def         gngrph_srch_datarepo_qry_fetch(gnsrch_ops, gnp_spark, srchfilter):

    if (gnsrch_ops.srch_init_data_status() == 0):
        rJ = {}
        rJ["nodes"] = []
        rJ["edges"] = []
        return(rJ)
    
    nodesonly = 0
    (njson, edgesjson) = gngrp_srch_qry_api(gnsrch_ops, srchfilter, nodesonly)
    
    nDF = gnp_spark.createDataFrame(njson)
    #resDF = nDF.filter(nDF.gnnodetype != "GNDataNode")\
    resDF = nDF.select(col("gnnodeid").alias("id"), \
                col("gnnodetype").alias("nodetype"), \
                col("gnlabel").alias("nodename"))

    res = resDF.toJSON().map(lambda j: json.loads(j)).collect()
    rJ = {}
    rJ["nodes"] = res

    edgResDF = gnp_spark.createDataFrame(edgesjson)
    eResDF = edgResDF.select(col("gnedgeid").alias("id"), \
                              col("gnedgetype").alias("type"), \
                              col("gnsrcnodeid").alias("source"), \
                              col("gntgtnodeid").alias("target"))
    ###eResDF.show(10)
    eres = eResDF.toJSON().map(lambda j: json.loads(j)).collect()
    ###eres = eResDF.toJSON().first()
    ###eres = eResDF.toJSON().first()
    rJ["edges"] = eres

    return(rJ)



    
    

def        gngrph_srch_metarepo_nodes_edges_fetch(gnsrch_ops, gnp_spark, srchfilter):


    
    sqlst = "select * from gnmetanodes WHERE gnnodetype='GNMetaNode' OR gnnodetype='GNMetaNodeAttr'"
    
    nodesonly = 1
    (njson, edgesjson) = gngrph_srch_metarepo_qry_api(gnsrch_ops, gnp_spark, sqlst, nodesonly)

    if (gnsrch_ops.srch_init_meta_status() == 0):
        rj={}
        rj["nodes"]=[]
        rj["edges"]=[]
        return rj
        
    nDF = gnp_spark.createDataFrame(njson)
    #resDF = nDF.filter(nDF.gnnodetype != "GNDataNode")\
    resDF = nDF.select(col("gnnodeid").alias("id"), \
                col("gnnodetype").alias("nodetype"), \
                col("gnnodename").alias("nodename"))
    
    res = resDF.toJSON().map(lambda j: json.loads(j)).collect()
    rJ = {}
    rJ["nodes"] = res

    edgResDF = gnp_spark.createDataFrame(edgesjson)
    eResDF = edgResDF.select(col("gnedgeid").alias("id"), \
                              col("gnedgetype").alias("type"), \
                              col("gnsrcnodeid").alias("source"), \
                              col("gntgtnodeid").alias("target"))
    ###eResDF.show(10)
    eres = eResDF.toJSON().map(lambda j: json.loads(j)).collect()
    ###eres = eResDF.toJSON().first()
    rJ["edges"] = eres    
        
    return(rJ)

    


if __name__ == "__main__":

    print("Starting gn ingest file")
    curDir = os.getcwd()
    rtDir = curDir.rsplit('/', 1)[0]
    app_name="gngraph"

    if rtDir not in sys.path:
        sys.path.append(rtDir)


    gndata_folder="/home/jovyan/GnanaDiscover/GnanaPath/gndata"
    gngraph_creds_folder = "/home/jovyan/GnanaDiscover/GnanaPath/creds/gngraph"
        
    sqlst = "SELECT * from customer"                
    
    ### Set spark session
    gnp_spark = SparkSession.builder.appName(app_name).getOrCreate()
    nodesonly = 0
    accessmode={'sfmode': 1, 'dbmode':1 }
    gngrph_cls = gngrph_search_init(gnp_spark, gndata_folder, gngraph_creds_foler, accessmode)
    
    (nJSON, eJSON) = gngrp_srch_qry_api(gngrph_cls, sqlst,  nodesonly)
    rfile="nodes.json" 
    with open(rfile, 'w') as fp:
            json.dump(nJSON, fp)

    efile="edges.json"
    with open(efile, "w") as fp:
            json.dump(eJSON, fp)

