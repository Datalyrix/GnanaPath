import psycopg2
from sqlalchemy import create_engine, exc
import json
from io import StringIO
import pandas as pds
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # Need for CREATE DATABASE
from psycopg2 import sql
from gnutils.gn_log import gn_log, gn_log_err


"""
      New Gngraph DB Initialize method create new gngraph database  and setup schemas, tables. The following schemas and tables are created- 
"""

class       GNGraphPgresDBMgmtOps:

    @classmethod
    def from_json(cls, dbname, json_file="gngraph_pgresdb_creds.json"):

        auth_file = PurePath(__file__).parents[0].joinpath(json_file)

        with open(auth_file, encoding='utf-8') as fh:
           gdb_creds = json.load(fh)

        return cls(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"], dbname, gdb_creds["dbtype"])

    
    @classmethod
    def from_args(cls, dbserver, dbport, dbuser, dbpasswd, dbname, dbtype):
        return cls(dbserver, dbport, dbuser, dbpasswd, dbname, dbtype)


    def __init__(self, dbserver, dbport, dbuser, dbpasswd, dbname, dbtype):
            self.dbuser = dbuser
            self.dbpasswd = dbpasswd
            self.dbserver = dbserver
            self.dbport = dbport
            self.dbname = dbname

            ##
            self.gnnode_table = "gnnodes"
            self.gnbizrules_table = "gnbizrules"
            self.gnmeta_schema = "gnmeta"
            self.gnedge_table = "gnedges"
            self.dbtype = dbtype


    
    def   db_connect(self):
        try:
            #Create an engine instance
            pgres_connstr='postgresql+psycopg2://'+self.dbuser+':'+self.dbpasswd+'@'+self.dbserver+':'+self.dbport+'/'+self.dbname
            print('PgresDBOps: Init ')
            print(pgres_connstr)
            alchemyEngine   = create_engine(pgres_connstr, pool_recycle=3600)
            # Connect to PostgreSQL server
            self.dbEngine = alchemyEngine
            self.dbConnp = alchemyEngine.connect();
            self.connected = 1
            print('pgresDBOps: Connected  ')
            print(self.dbConnp)
        except exc.SQLAlchemyError as err:
            self.dbConnp = None
            self.connected = 0
            print('gngraphPgresDBOps: unable to connect pgres ')
            print(err)

        return self.connected
 

            
    def   db_create_database_1(self, dbname):

       ###self.dbConnp.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # Need AUTOCOMMIT for CREATE DATABASE
       ##cur = con.cursor()
       self.dbConnp.autocommit = True 
       #create database first
       sql_cmd = f"CREATE DATABASE "+dbname+";"
       self.dbEngine.execute(sql_cmd)

       
    def   db_create_schema_1(self, schemaname):
       #create schema 
       sql_cmd = f"CREATE SCHEMA  "+schemaname+";"
       self.dbEngine.execute(sql_cmd)

    def   db_create_table(self, tabl_sql_str):
       #create table
       try:
           
            self.dbEngine.execute(tabl_sql_str)
            self.dbEngine.commit()
            return 0
       except exc.SQLAlchemyError as err:
            print('gngraphPgresDBOps: Error creating table ')
            print(err)
            return -1

    def    db_create_schema(self, db_name, schema_name):

       try:
            con = psycopg2.connect(dbname=db_name,
                                user=self.dbuser, host=self.dbserver, port=self.dbport,
                                password=self.dbpasswd)

            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # <-- ADD THIS LINE
            cur = con.cursor()

            # Use the psycopg2.sql module instead of string concatenation
            # in order to avoid sql injection attacs.
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                       sql.Identifier(schema_name))
                      )
            con.close()
            return 0
       except:
           print("ERROR ")
           return -1

       
    def    db_create_database(self, newdbname):

       try: 
            con = psycopg2.connect(dbname='postgres',
                                user=self.dbuser, host=self.dbserver, port=self.dbport,
                                password=self.dbpasswd)

            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # <-- ADD THIS LINE         
            cur = con.cursor()
            # Use the psycopg2.sql module instead of string concatenation 
            # in order to avoid sql injection attacs.
            # Psql does not support IF NOT EXISTS for database
            ##sql_cmd="SELECT 'CREATE DATABASE "+newdbname+"' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '"+newdbname+"')\gexec"
            cur.execute(sql.SQL("CREATE DATABASE {}").format(
                       sql.Identifier(newdbname))
                      )
            ###cur.execute(sql.SQL(sql_cmd))
            con.close()
            return 0
       except psycopg2.OperationalError as err:
           gn_log(" CREATE DATABASE ERROR ")
           gn_log(err)
           return -1
       except psycopg2.Error as err:
           gn_log('CREATE DATABASE ERROR  ')
           gn_log(err)
           return -1

    def  gngraph_db_datanode_create_table(self, dnodename, schema):
        gn_log('GNGrphDBMgmtOps: create table for datanode '+dnodename)
        tbl_str = schema+"."+dnodename.lower()
        
        dnode_tablestr = "CREATE TABLE IF NOT EXISTS "+tbl_str+" (gnnodeid bigint NOT NULL PRIMARY KEY, gnnodetype text, gnmetanodeid bigint,  gndatanodeprop json,  gndatanodeobj json, uptmstmp timestamp);"
        print('gnMgmtOps  table_str '+dnode_tablestr)
        self.db_create_table(dnode_tablestr)
        gn_log('GNGrphDBMgmtOps: table '+tbl_str+' is created ');
       
    def   gngraph_db_initialize(self, newdbname, iscreatedb):

        gn_log('GNGraphDBInit: Initializing Graph on database '+newdbname)
        ## First check connecting to postgres
        self.dbname = "postgres"
        
        ##connected = self.db_connect()
        ##if (connected == 0):
        ##    print('GNGraphDBInit: ERROR Failed to connect to DB ')
        ##    return -1

        ## first create gngraph
        if (iscreatedb == 1):
          res = self.db_create_database(newdbname)

          if (res < 0):
               gn_log('GNGraphDBInit: ERROR Failed to create new database : '+newdbname)
               return res
          gn_log('GNGraphDBInit: database '+newdbname+' is created ')
          
        ## Now connect using new database
        self.dbname = newdbname
        connected = self.db_connect()
        if (connected == 0):
            gn_log('GNGraphDBInit: Failed to connect new database:'+newdbname)
            return -1
    
        ### create schema
        self.db_create_schema(newdbname, "gnmeta")
        gn_log('GNGraphDBInit: gnmeta schema created ')
        
        gnnode_tablestr = "CREATE TABLE IF NOT EXISTS gnmeta.gnnodes (gnnodeid bigint NOT NULL PRIMARY KEY, gnnodename text, gnnodetype text, gnnodeprop json, uptmstmp timestamp);"
        #print('GNPgreDBMgmtOps: create table '+gnnode_tablestr) 
        self.db_create_table(gnnode_tablestr)
        gn_log('GNGraphDBInit: table gnnodes is created ')
        
        gnedge_tablestr = "CREATE TABLE IF NOT EXISTS gnmeta.gnedges ( gnedgeid bigint NOT NULL PRIMARY KEY, gnedgename text, gnedgetype text, gnsrcnodeid bigint,  gntgtnodeid bigint, gnedgeprop json, uptmstmp timestamp);"
        ##print('GNPgreDBMgmtOps: create table '+gnedge_tablestr)
        self.db_create_table(gnedge_tablestr)
        gn_log('GNGraphDBInit: table gnedges is created ')
        
        gnbizrules_tablestr = "CREATE TABLE IF NOT EXISTS gnmeta.gnbizrules ( gnrelid bigint NOT NULL PRIMARY KEY, gnrelname text, gnreltype text, gnsrcnodeid  bigint, gntgtnodeid  bigint, gnmatchnodeid bigint, gnrelprop json, gnrelobj json, state text, freq text, uptmstmp  timestamp);"
        #print('GNPgreDBMgmtOps: create table '+gnbizrules_tablestr)
        self.db_create_table(gnbizrules_tablestr)
        gn_log('GNGraphDBInit: table gnbizrules is created ')
        
        ## Create default Business Domains
        self.db_create_schema(newdbname, "customer")
        self.db_create_schema(newdbname, "product")
        self.db_create_schema(newdbname, "sales")
        gn_log('GNGraphDBInit: schemas customer, product, sales are created ')
        gn_log('GNGraphDBInit: GNGraph Intialization Successful ')
        return 0




def     gngrph_pgres_get_connection_status(pgres_conf):

    pgdb_cls = GNGraphPgresDBOps(pgres_conf['serverIP'], pgres_conf['serverPort'], pgres_conf['username'], pgres_conf['password'], dbname, "")
    is_connect = pgdb_cls._isconnected()

    if (is_connect == 1):
        connection_status = "Connected"
    else:
        connection_status = "NotConnected"

    return connection_status
