import os
from os import path
import pandas as pds
import json
import pathlib
from pyspark.sql.functions import from_json, col

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
            self.__gnmetaNodeDF = self.__spark.read.json(self.gnmetanode_filepath)
            self.__gnmetaEdgeDF = self.__spark.read.json(self.gnmetaedge_filepath)
            self.__gnmetaNodeDF.createOrReplaceTempView("gnmetanodes")
            self.__gnmetaNodeDF.createOrReplaceTempView("gnmetaedges")
            
            
    def  get_metanode_info(self, node, spk):


       if spk is None:
           print('GNStaticFileOps: spark is none ')
           
       sqlstr = "SELECT * FROM gnmetanodes where gnnodename='"+node+"'"
       print('get_metanode_info:  sqlstr '+sqlstr)
       nodeEnt = spk.sql(sqlstr)
       nodeEnt.show()
       jobj = json.loads(nodeEnt.toJSON().first())       
       return jobj
                       
    def datanode_flatten_jsonfields(self, baseDataNodeDF, spk):

        # First flatten gndatanodeobj
        n1_schema = spk.read.json(baseDataNodeDF.rdd.map(lambda row: row.gndatanodeobj)).schema
        n1DF = baseDataNodeDF.withColumn("gndatanodeobj", from_json("gndatanodeobj", n1_schema))\
                             .select(col('gnnodeid'), col('gnnodetype'), col('gnmetanodeid'), col('uptmstmp'), col('gndatanodeobj.*'))
        # Flatten gndatanodeprop
        n2_schema = spk.read.json(baseDataNodeDF.rdd.map(lambda row: row.gndatanodeprop)).schema
        n2DF = baseDataNodeDF.withColumn("gndatanodeprop", from_json("gndatanodeprop", n2_schema))\
                             .select(col('gnnodeid'), col('gndatanodeprop.*'))

        datanodeFlattenDF = n1DF.join(n2DF, n1DF.gnnodeid==n2DF.gnnodeid, "inner")

        return datanodeFlattenDF

        
    def   get_datanode_mapped_df(self, node_name, bizdomain, spk):

        # map the datanode file to spark datafram
        nodefile = node_name+".json"
        dnode_fpath = self.gndata_graph_data_folder+"/"+bizdomain+"/"+node_name+"/"+nodefile

        retDF = None
        
        if path.exists(dnode_fpath):
           dnodeDF = spk.read.json(dnode_fpath)
           dnodeDF.show(1)
           # flatten gndatanodeprop and gndatanodeobj (actual dataset attibutes)
           retDF = self.datanode_flatten_jsonfields(dnodeDF, spk)
           # also map the node to tempview with nodename
           retDF.createOrReplaceTempView(node_name)
           
        return retDF


    def   gnmetaedges_map_df(self, spk):

        # map the datanode file to spark datafram
        edgefile = "gnmetaedges.json"
        edge_fpath = self.gndata_graph_data_folder+"/"+edgefile

        retDF = None
        if path.exists(edge_fpath):
           metaEdgeDF = spk.read.json(edge_fpath)
           metaEdgeDF.show(1)
           # flatten gnedgeprop  (actual dataset attibutes)
           edge_schema = spk.read.json(metaEdgeDF.rdd.map(lambda row: row.gnedgeprop)).schema
           self.__gnmetaEdgeDF = metaEdgeDF.withColumn("gnedgeprop", from_json("gnedgeprop", edge_schema)).select(col('gnedgeid'), col('gnedgename'), col('gnedgetype'), col('gnsrcnodeid'), col('gntgtnodeid'),  col('gnedgeprop.*'))
           
           # also map the node to tempview with nodename
           self.__gnmetaEdgeDF.createOrReplaceTempView("gnmetaedges")
        return self.__gnmetaEdgeDF

    

    def   gnmetanodes_map_df(self, spk):

        # map the datanode file to spark dataframe
        mnodefile = "gnmetanodes.json"
        mnodes_fpath = self.gndata_graph_data_folder+"/"+mnodefile

        retDF = None
        if path.exists(mnodes_fpath):
           metaNodeDF = spk.read.json(mnodes_fpath)
           metaNodeDF.show(1)
           # flatten gnedgeprop  (actual dataset attibutes)
           mnode_schema = spk.read.json(metaNodeDF.rdd.map(lambda row: row.gnnodeprop)).schema
           self.__gnmetaNodeDF = metaNodeDF.withColumn("gnnodeprop", from_json("gnnodeprop", mnode_schema)).select(col('gnnodeid'), col('gnnodename'), col('gnnodetype'), col('gnnodeprop.*'))

           # also map the node to tempview with nodename
           self.__gnmetaNodeDF.createOrReplaceTempView("gnmetanodes")
        return self.__gnmetaNodeDF



