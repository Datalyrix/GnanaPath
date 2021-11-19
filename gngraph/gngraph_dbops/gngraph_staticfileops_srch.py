import os
from os import path
import pandas as pds
import json
import pathlib
from pyspark.sql.functions import from_json, col
from gnappsrv.gn_config import gn_log, gn_log_err

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
            self.gnmetanode_filepath = self.gndata_graph_data_folder+"/"+"gnmetanodes.json"
            self.gnmetaedge_filepath = self.gndata_graph_data_folder+"/"+"gnmetaedges.json"
            try:
                self.__gnmetaNodeDF = self.__spark.read.json(self.gnmetanode_filepath)    
                self.__gnmetaEdgeDF = self.__spark.read.json(self.gnmetaedge_filepath)
                self.__gnmetaNodeDF.createOrReplaceTempView("gnmetanodes")
                self.__gnmetaNodeDF.createOrReplaceTempView("gnmetaedges")
                self.__init_failed = 0
            except:
                gn_log_err('GNStaticFileOps:  error reading file '+self.gnmetanode_filepath)
                self.__init_failed = 1
            
    def  get_metanode_info(self, node, spk):

       jobj = {}
       
       if spk is None:
           gn_log_err('GNStaticFileOps: spark session is not found')
           return jobj

       if self.__init_failed == 1:
           gn_log_err('GNStaticFileOps: Static Files Init failed ')
           return jobj
       
       sqlstr = "SELECT * FROM gnmetanodes where gnnodename='"+node+"'"
       print('get_metanode_info:  sqlstr '+sqlstr)
       nodeEnt = spk.sql(sqlstr)
       ##nodeEnt.show()
       jobj = json.loads(nodeEnt.toJSON().first())       
       return jobj
                       
    def datanode_flatten_jsonfields(self, baseDataNodeDF, spk):

        # First flatten gndatanodeobj
        n1_schema = spk.read.json(baseDataNodeDF.rdd.map(lambda row: row.gndatanodeobj)).schema
        n2_schema = spk.read.json(baseDataNodeDF.rdd.map(lambda row: row.gndatanodeprop)).schema

        
        datanodeFlattenDF = baseDataNodeDF.withColumn("gndatanodeobj", from_json("gndatanodeobj", n1_schema)) \
                          .withColumn("gndatanodeprop", from_json("gndatanodeprop", n2_schema)) \
                          .select(col("gnnodeid"), col("gnnodetype"), col("gnmetanodeid"), col("gndatanodeobj.*"), col("gndatanodeprop.*"), col("uptmstmp"))

        return datanodeFlattenDF

        
    def   get_datanode_mapped_df(self, node_name, bizdomain, spk):

        # map the datanode file to spark datafram
        nodefile = node_name+".json"
        dnode_fpath = self.gndata_graph_data_folder+"/"+bizdomain+"/"+node_name+"/"+nodefile

        retDF = None
        
        if path.exists(dnode_fpath):
           retDF = spk.read.json(dnode_fpath)
           ##retDF.show(1)
           # flatten gndatanodeprop and gndatanodeobj (actual dataset attibutes)
           dnodeDF = self.datanode_flatten_jsonfields(retDF, spk)
           # also map the node to tempview with nodename
           dnodeDF.createOrReplaceTempView(node_name)
           
        return dnodeDF


    def   gnmetaedges_map_df(self, spk):

        # map the datanode file to spark datafram
        edgefile = "gnmetaedges.json"
        edge_fpath = self.gndata_graph_data_folder+"/"+edgefile

        retDF = None
        if path.exists(edge_fpath):
            metaEdgeDF = spk.read.json(edge_fpath)
            ####metaEdgeDF.show(1)
            # flatten gnedgeprop  (actual dataset attibutes)
            edge_schema = spk.read.json(metaEdgeDF.rdd.map(lambda row: row.gnedgeprop)).schema
            self.__gnmetaEdgeDF = metaEdgeDF.withColumn("gnedgeprop", from_json("gnedgeprop", edge_schema)).select(col('gnedgeid'), col('gnedgename'), col('gnedgetype'), col('gnsrcnodeid'), col('gntgtnodeid'),  col('gnedgeprop.*'))
           
            # also map the node to tempview with nodename
            self.__gnmetaEdgeDF.createOrReplaceTempView("gnmetaedges")
        else:
            self.__gnmetaEdgeDF = None
            
        return self.__gnmetaEdgeDF

    

    def   gnmetanodes_map_df(self, spk):

        # map the datanode file to spark dataframe
        mnodefile = "gnmetanodes.json"
        mnodes_fpath = self.gndata_graph_data_folder+"/"+mnodefile

        retDF = None
        if path.exists(mnodes_fpath):
           metaNodeDF = spk.read.json(mnodes_fpath)
           ##metaNodeDF.show(1)
           # flatten gnedgeprop  (actual dataset attibutes)
           mnode_schema = spk.read.json(metaNodeDF.rdd.map(lambda row: row.gnnodeprop)).schema
           self.__gnmetaNodeDF = metaNodeDF.withColumn("gnnodeprop", from_json("gnnodeprop", mnode_schema)).select(col('gnnodeid'), col('gnnodename'), col('gnnodetype'), col('gnnodeprop.*'))

           # also map the node to tempview with nodename
           self.__gnmetaNodeDF.createOrReplaceTempView("gnmetanodes")
        else:
            self.__gnmetaNodeDF = None
            
        return self.__gnmetaNodeDF

    def  get_bizrule_metainfo(self, bizrid, spk):

       if spk is None:
          print('GNPgresSrchOps: spark is none ')

       sqlstr = "SELECT * FROM gnbizrules where gnrelid='"+bizrid+"'"
       print('GNPgresSrchOps: get_bizrule_metainfo:  sqlstr '+sqlstr)
       bizrEnt = spk.sql(sqlstr)
       #bizrEnt.show()
       jobj = json.loads(bizrEnt.toJSON().first())
       return jobj


