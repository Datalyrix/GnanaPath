import os
from os import path
import pandas as pds
import json
import pathlib
from pyspark.sql.functions import from_json, col
from gnutils.gn_srch_log import gnsrch_log, gnsrch_log_err

"""
gngraph static fileops implementation for search (spark-based) 
this class will map all static files as spark dataframes for queries

"""


class  GNGraphSrchStaticFileOps:


    def __init__(self, gndata_graph_folder, sp):
        
            self.gndata_graph_folder = gndata_graph_folder
            self.gndata_graph_data_folder = gndata_graph_folder+"/data"
            self.gndata_graph_config_folder = gndata_graph_folder+"/config"
            self.gnnode_table = "gnnodes"
            self.gnmeta_schema = "gnmeta"
            self.gnedge_table = "gnedges"
            self.__spark = sp
            self.__gnmetaEdges_mapped = 0
            self.__gnmetaNodes_mapped  = 0
            
            self.gnmetanode_filepath = self.gndata_graph_data_folder+"/"+"gnmetanodes.json"
            self.gnmetaedge_filepath = self.gndata_graph_data_folder+"/"+"gnmetaedges.json"
            try:
                self.gnmetanodes_map_df(self.__spark)
                self.gnmetaedges_map_df(self.__spark)
            #try:
            #    self.__gnmetaNodeDF = self.__spark.read.json(self.gnmetanode_filepath)    
            #    self.__gnmetaEdgeDF = self.__spark.read.json(self.gnmetaedge_filepath)
            #    self.__gnmetaNodeDF.createOrReplaceTempView("gnmetanodes")
            #    self.__gnmetaNodeDF.createOrReplaceTempView("gnmetaedges")
                self.__init_failed = 0
            except:
                gnsrch_log_err('GNStaticFileOps:  error reading file '+self.gnmetanode_filepath)
                self.__init_failed = 1
            
    def  get_metanode_info(self, node, spk):

       jobj = {}
       
       if spk is None:
           gnsrch_log_err('GnStaticFOps: spark session is not found')
           return jobj

       if self.__init_failed == 1:
           gnsrch_log_err('GNStaticFileOps: Static Files Init failed ')
           return jobj
       
       sqlstr = "SELECT * FROM gnmetanodes where gnnodename='"+node+"'"
       gnsrch_log('GnStaticFOps:  getting metanode info '+node+' sqlstr '+sqlstr)
       nodeEnt = spk.sql(sqlstr)
       nodeCount = nodeEnt.count()
       if (nodeCount == 0):
           gnsrch_log('GnStaticFOps: node '+node+' does not exists')
           return jobj
       
       ##nodeEnt.show()
       jobj = json.loads(nodeEnt.toJSON().first())       
       return jobj
                       
    def datanode_flatten_jsonfields(self, baseDataNodeDF, spk):
        gnsrch_log('GnStaticFOps: Flattening datanode fields: ')

        try:
            # First flatten gndatanodeobj
            n1_df = spk.read.json(baseDataNodeDF.rdd.map(lambda row: row.gndatanodeobj))
        
            n1_df.show(2)
            n1_schema = n1_df.schema
            gnsrch_log('GnStaticFOps: Flattening n1_schema datanodeobj ')
            gnsrch_log(n1_schema)
            n2_schema = spk.read.json(baseDataNodeDF.rdd.map(lambda row: row.gndatanodeprop)).schema
            gnsrch_log('  Flattening n2_schema datanodeprop ')
            gnsrch_log(n2_schema)
        
            datanodeFlattenDF = baseDataNodeDF.withColumn("gndatanodeobj", from_json("gndatanodeobj", n1_schema)) \
                          .withColumn("gndatanodeprop", from_json("gndatanodeprop", n2_schema)) \
                          .select(col("gnnodeid"), col("gnnodetype"), col("gnmetanodeid"), col("gndatanodeobj.*"), col("gndatanodeprop.*"), col("uptmstmp"))

        except Exception as err:
            gnsrch_log_err('GnStaticFOps: Flatten fields ran into Exception '+str(err))
            datanodeFlattenDF = None

            
        return datanodeFlattenDF

        
    def   get_datanode_mapped_df(self, node_name, bizdomain, spk):

        # map the datanode file to spark datafram
        nodefile = node_name+".json"
        dnode_fpath = self.gndata_graph_data_folder+"/"+bizdomain+"/"+node_name+"/"+nodefile

        retDF = None
        
        if path.exists(dnode_fpath):
           retDF = spk.read.json(dnode_fpath)
           retDF.show(2)
           # flatten gndatanodeprop and gndatanodeobj (actual dataset attibutes)
           dnodeDF = self.datanode_flatten_jsonfields(retDF, spk)
           # also map the node to tempview with nodename
           dnodeDF.createOrReplaceTempView(node_name)
           
        return dnodeDF


    def   gnmetaedges_map_df(self, spk):
        
        # map the datanode file to spark dataframe
        edgefile = "gnmetaedges.json"
        edge_fpath = self.gndata_graph_data_folder+"/"+edgefile
        gnsrch_log('GnStaticFOps: Mapping gnmetaedges ')

        #if we already mapped df just return
        if (self.__gnmetaEdges_mapped == 1):
            print('GnStaticFOps: gnmetaedges are already mapped ')
            return self.__gnmetaEdgeDF
        
        retDF = None
        if path.exists(edge_fpath):
            metaEdgeDF = spk.read.json(edge_fpath)
            metaEdgeDF.show(2)
            edge_schema = spk.read.json(metaEdgeDF.rdd.map(lambda row: row.gnedgeprop)).schema
            print('GnStaticFOps: showing gnmetaedge schema ')
            print(edge_schema)
            self.__gnmetaEdgeDF = metaEdgeDF.withColumn("gnedgeprop", from_json("gnedgeprop", edge_schema)).select(col('gnedgeid'), col('gnedgename'), col('gnedgetype'), col('gnsrcnodeid'), col('gntgtnodeid'),  col('gnedgeprop.*')) 
            self.__gnmetaEdgeDF.show(2)
            # also map the node to tempview with nodename
            self.__gnmetaEdgeDF.createOrReplaceTempView("gnmetaedges")
            self.__gnmetaEdges_mapped = 1
        else:
            self.__gnmetaEdgeDF = None
            self.__gnmetaEdges_mapped = 0
            
        gnsrch_log('GnStaticFOps: gnmetaedges are mapped to df SUCCESS')    
        return self.__gnmetaEdgeDF

    

    def   gnmetanodes_map_df(self, spk):

        # map the datanode file to spark dataframe
        mnodefile = "gnmetanodes.json"
        mnodes_fpath = self.gndata_graph_data_folder+"/"+mnodefile
        gnsrch_log('GnStaticFOps: Mapping gnmetanodes ')

        if (self.__gnmetaNodes_mapped == 1):
            gnsrch_log('GnStaticFOps: gnmetanodes are already mapped ')
            return self.__gnmetaNodeDF
        
        retDF = None
        if path.exists(mnodes_fpath):
           metaNodeDF = spk.read.json(mnodes_fpath)
           metaNodeDF.show(2)
           mnode_schema = spk.read.json(metaNodeDF.rdd.map(lambda row: row.gnnodeprop)).schema
           #gnsrch_log(mnode_schema)
           self.__gnmetaNodeDF = metaNodeDF.withColumn("gnnodeprop", from_json("gnnodeprop", mnode_schema)).select(col('gnnodeid'), col('gnnodename'), col('gnnodetype'), col('gnnodeprop.*'))
           self.__gnmetaNodeDF.show(2)
           # also map the node to tempview with nodename
           self.__gnmetaNodeDF.createOrReplaceTempView("gnmetanodes")
           self.__gnmetaNodes_mapped = 1
        else:
            self.__gnmetaNodeDF = None
        gnsrch_log("GnStaticFOps: gnmetanodes are mapped to df SUCCESS")    
        return self.__gnmetaNodeDF

    def  get_bizrule_metainfo(self, bizrid, spk):

       if spk is None:
          gnsrch_log_err('GnStaticFOps: spark is none ')

       sqlstr = "SELECT * FROM gnbizrules where gnrelid='"+bizrid+"'"
       gnsrch_log('GnStaticFOps: get_bizrule_metainfo:  sqlstr '+sqlstr)
       bizrEnt = spk.sql(sqlstr)
       #bizrEnt.show()
       jobj = json.loads(bizrEnt.toJSON().first())
       return jobj


