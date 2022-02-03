import os
import sys
from os import path
import pandas as pds
import json
import pathlib
from pyspark.sql.functions import from_json, col
import psycopg2
from sqlalchemy import create_engine


curentDir = os.getcwd()
parentDir = curentDir.rsplit('/', 1)[0]
if parentDir not in sys.path:
    sys.path.append(parentDir)

gnRootDir = parentDir.rsplit('/', 1)[0]
if gnRootDir not in sys.path:
    sys.path.append(gnRootDir)

from gnutils.gn_srch_log import gnsrch_log, gnsrch_log_err

"""
gngraph db srch implementation main class and associated functions
current implementation uses postgres as underlying database 

"""


class       GNGraphSrchPgresDBOps:

    @classmethod
    def from_json(cls, dbname, spk, json_file="gngraph_pgresdb_creds.json"):

        auth_file = PurePath(__file__).parents[0].joinpath(json_file)

        with open(auth_file, encoding='utf-8') as fh:
           gdb_creds = json.load(fh)

        return cls(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"], dbname, gdb_creds["dbtype"], spk)

    @classmethod
    def from_args(cls, dbserver, dbport, dbuser, dbpasswd, dbname, spk):
        return cls(dbserver, dbport, dbuser, dbpasswd, dbname, spk)

    def __init__(self, dbserver, dbport, dbuser, dbpasswd, dbname, spk):
        
            #Create an engine instance
            self.__gdb_dbserver = dbserver
            self.__gdb_dbport = dbport
            self.__gdb_dbuser = dbuser
            self.__gdb_dbpasswd = dbpasswd
            self.__gdb_dbname = dbname
            ##self.__gdb_metadb = "gngraph_db"
            ##self.__gdb_datadb = "gngraph_db"
            self.__gdb_metadb_schema = "gnmeta"
            self.__gdb_metanodes_tbl = "gnnodes"
            self.__gdb_metaedges_tbl = "gnedges"
            self.__spark = spk
            self.gnmetanodes_map_dataframe(spk) 


    def    gngrph_tblmap_dataframe(self, dbname, schema, tbl_name, spk):

            gnsrch_log('GnSrchPgresDBOps: map node tabl to  dataframe')
            db_connstr = "jdbc:postgresql://"+self.__gdb_dbserver+":"+self.__gdb_dbport+"/"+dbname
            node_tbl = schema+'."'+tbl_name+'"'
            ###datadb_connstr = "jdbc:postgresql://"+self.__gdb_dbserver+":"+self.__gdb_dbport+"/"+self.__gdb_datadb

            gnsrch_log('GnSrchPgresDBOps:    dbconnstr:'+db_connstr)
            gnsrch_log('GnSrchPgresDBOps:    node_tbl '+node_tbl)

            try:
                retDF = spk.read \
                           .format("jdbc") \
                           .option("url", db_connstr) \
                           .option("dbtable", node_tbl) \
                           .option("user", self.__gdb_dbuser) \
                           .option("password", self.__gdb_dbpasswd) \
                           .option("driver", "org.postgresql.Driver") \
                           .load()
                ###retDF.show(2)
                dnodeDF = self.datanode_flatten_jsonfields(retDF, spk)
                
                retDF.createOrReplaceTempView(tbl_name)
                print('GnSrchPgresDBOps: '+tbl_name+' mapped dataframe');
            except Exception as error :
                print('GnSrchPgresDBOps: ERROR failed')
                print(error)
                retDF = None
                
            return retDF



            
    def    gnmetanodes_map_dataframe(self, spk):        

            gnsrch_log('GnSrchPgresDBOps: map metanode dataframe')
            metadb_connstr = "jdbc:postgresql://"+self.__gdb_dbserver+":"+self.__gdb_dbport+"/"+self.__gdb_dbname            
            metanode_tbl = self.__gdb_metadb_schema+"."+self.__gdb_metanodes_tbl            
            datadb_connstr = "jdbc:postgresql://"+self.__gdb_dbserver+":"+self.__gdb_dbport+"/"+self.__gdb_dbname
            
            try:   
                self.__gnmetaNodeDF = spk.read \
                                       .format("jdbc") \
                                       .option("url", metadb_connstr) \
                                       .option("dbtable", metanode_tbl) \
                                       .option("user", self.__gdb_dbuser) \
                                       .option("password", self.__gdb_dbpasswd) \
                                       .option("driver", "org.postgresql.Driver") \
                                       .load()
                ####self.__gnmetaNodeDF.show(2)
                self.__gnmetaNodeDF.createOrReplaceTempView("gnmetanodes")
                gnsrch_log('GnSrchPgresDbOps: mapped dataframe COMPLETED ');
            except Exception as error :
                gnsrch_log('GnSrchPgresDbOps: ERROR failed')
                gnsrch_log(error)
                self.__gnmetaNodeDF = None


    def    gnmetaedges_map_df(self, spk):

            gnsrch_log('GnSrchPgresDBOps: map metaedges dataframe')
            metadb_connstr = "jdbc:postgresql://"+self.__gdb_dbserver+":"+self.__gdb_dbport+"/"+self.__gdb_dbname
            metaedge_tbl = self.__gdb_metadb_schema+"."+self.__gdb_metaedges_tbl
            ###datadb_connstr = "jdbc:postgresql://"+self.__gdb_dbserver+":"+self.__gdb_dbport+"/"+self.__gdb_datadb
            gnsrch_log('GnSrchPgresDBOps: connecting database table: '+metaedge_tbl)
            
            try:
                metaEdgeDF = spk.read \
                                    .format("jdbc") \
                                    .option("url", metadb_connstr) \
                                    .option("dbtable", metaedge_tbl) \
                                    .option("user", self.__gdb_dbuser) \
                                    .option("password", self.__gdb_dbpasswd) \
                                    .option("driver", "org.postgresql.Driver") \
                                    .load()
                ###metaEdgeDF.show(2)
                # also flatten json objects
                print('GNPgressSrchOps: metaedgeDF mapped ')
                edge_schema = spk.read.json(metaEdgeDF.rdd.map(lambda row: row.gnedgeprop)).schema
                self.__gnmetaEdgeDF = metaEdgeDF.withColumn("gnedgeprop", from_json("gnedgeprop", edge_schema)).select(col('gnedgeid'), col('gnedgename'), col('gnedgetype'), col('gnsrcnodeid'), col('gntgtnodeid'),  col('gnedgeprop.*'))
                
              
                self.__gnmetaEdgeDF.createOrReplaceTempView("gnmetaedges")
                print('GNPgresSrchOps: mapped dataframe COMPLETED ');
            except Exception as error :
                print('GNPgresSrchOps: ERROR failed')
                print(error)
                self.__gnmetaEdgeDF = None

            return self.__gnmetaEdgeDF




    def    gnmetanodes_map_df(self, spk):

            gnsrch_log('GnSrchPgresDBOps: mapping metanodes to a dataframe')
            metadb_connstr = "jdbc:postgresql://"+self.__gdb_dbserver+":"+self.__gdb_dbport+"/"+self.__gdb_dbname
            metanodes_tbl = self.__gdb_metadb_schema+"."+self.__gdb_metanodes_tbl
            ###datadb_connstr = "jdbc:postgresql://"+self.__gdb_dbserver+":"+self.__gdb_dbport+"/"+self.__gdb_datadb

            gnsrch_log('GnSrchPgresDBOps: metadb connstr: '+metanodes_tbl);
            try:
                metaNodeDF = spk.read \
                                    .format("jdbc") \
                                    .option("url", metadb_connstr) \
                                    .option("dbtable", metanodes_tbl) \
                                    .option("user", self.__gdb_dbuser) \
                                    .option("password", self.__gdb_dbpasswd) \
                                    .option("driver", "org.postgresql.Driver") \
                                    .load()
                ####metaNodeDF.show(2)
                print('GNPgressSrchOps: metaNodeDF mapped ')
                # also flatten json objects

                node_schema = spk.read.json(metaNodeDF.rdd.map(lambda row: row.gnnodeprop)).schema
                self.__gnmetaNodeDF = metaNodeDF.withColumn("gnnodeprop", from_json("gnnodeprop", node_schema)).select(col('gnnodeid'), col('gnnodename'), col('gnnodetype'), col('gnnodeprop.*'))


                self.__gnmetaNodeDF.createOrReplaceTempView("gnmetanodes")
                print('GNPgresSrchOps: mapped dataframe COMPLETED ');
            except Exception as error :
                print('GNPgresSrchOps: ERROR failed')
                print(error)
                self.__gnmetaNodeDF = None

            return self.__gnmetaNodeDF
        
                
            
    def  get_metanode_info(self, node, spk):

       if spk is None:
          print('GNPgresSrchOps: spark is none ')

       sqlstr = "SELECT * FROM gnmetanodes where gnnodename='"+node+"'"
       print('GNPgresSrchOps: get_metanode_info:  sqlstr '+sqlstr)
       nodeEnt = spk.sql(sqlstr)
       ###nodeEnt.show()
       jobj = json.loads(nodeEnt.toJSON().first())
       return jobj


    def datanode_flatten_jsonfields(self, baseDataNodeDF, spk):

        # First flatten gndatanodeobj
        n1_schema = spk.read.json(baseDataNodeDF.rdd.map(lambda row: row.gndatanodeobj)).schema
        n2_schema = spk.read.json(baseDataNodeDF.rdd.map(lambda row: row.gndatanodeprop)).schema


        datanodeFlattenDF = baseDataNodeDF.withColumn("gndatanodeobj", from_json("gndatanodeobj", n1_schema)) \
                                          .withColumn("gndatanodeprop", from_json("gndatanodeprop", n2_schema)) \
                                          .select(col("gnnodeid"), col("gnnodetype"), col("gnmetanodeid"), col("gndatanodeobj.*"), col("gndatanodeprop.*"), col("uptmstmp"))
        

        print('GNPgresDBSrchOps: Flatten dataframe for JSON Objects')
        ###datanodeFlattenDF.show(2)
        #n1DF = baseDataNodeDF.withColumn("gndatanodeobj", from_json("gndatanodeobj", n1_schema))\
        #                     .select(col('gnnodeid'), col('gnnodetype'), col('gnmetanodeid'), col('uptmstmp'), col('gndatanodeobj.*'))
        # Flatten gndatanodeprop
        #n2_schema = spk.read.json(baseDataNodeDF.rdd.map(lambda row: row.gndatanodeprop)).schema
        #n2DF = baseDataNodeDF.withColumn("gndatanodeprop", from_json("gndatanodeprop", n2_schema))\
        ##                      .select(col('gndatanodeprop.*'))
        ##                    .select(col('gnnodeid'), col('gndatanodeprop.*'))
                    
        
        ##datanodeFlattenDF = n1DF.join(n2DF, n1DF.gnnodeid==n2DF.gnnodeid, "inner")        
        return datanodeFlattenDF

        
   
    def get_datanode_mapped_df(self, node_name, bizdomain, spk):

        # map the datanode file to spark dataframe
        gnsrch_log("GnSrchPgresDBOps: map "+node_name+" domain "+bizdomain+" ")
        
        dnodeDF = self.gngrph_tblmap_dataframe(self.__gdb_dbname, bizdomain, node_name, spk)
        
        retDF = None
        if dnodeDF is not None:
           ###dnodeDF.show(1)
           # flatten gndatanodeprop and gndatanodeobj (actual dataset attibutes)
           retDF = self.datanode_flatten_jsonfields(dnodeDF, spk)
           # also map the node to tempview with nodename
           retDF.createOrReplaceTempView(node_name)

        return retDF

    
    def  get_bizrule_metainfo(self, bizrid, spk):

       if spk is None:
          print('GNPgresSrchOps: spark is none ')

       sqlstr = "SELECT * FROM gnbizrules where gnrelid='"+bizrid+"'"
       print('GNPgresSrchOps: get_bizrule_metainfo:  sqlstr '+sqlstr)
       bizrEnt = spk.sql(sqlstr)
       #bizrEnt.show()
       jobj = json.loads(bizrEnt.toJSON().first())
       return jobj

