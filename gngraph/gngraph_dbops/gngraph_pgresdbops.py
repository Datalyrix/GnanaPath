import psycopg2
from sqlalchemy import create_engine, exc
import json
from io import StringIO
import pandas as pds
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # Need for CREATE DATABASE
from psycopg2 import sql
from gnutils.gn_log import gn_log, gn_log_err

"""
gngraph db implementation main class and associated functions
current implementation uses postgres as underlying database
"""


class       GNGraphPgresDBOps:

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
        try:
            #Create an engine instance
            pgres_connstr='postgresql+psycopg2://'+dbuser+':'+dbpasswd+'@'+dbserver+':'+dbport+'/'+dbname
            gn_log('PgresDBOps: setting up connection ')
            alchemyEngine   = create_engine(pgres_connstr, pool_recycle=3600)
            # Connect to PostgreSQL server
            self.dbEngine = alchemyEngine
            self.dbConnp = alchemyEngine.connect();
            
            self.__gdb_dbuser = dbuser
            self.__gdb_dbpasswd = dbpasswd
            self.__gdb_dbserver = dbserver
            self.__gdb_dbport = dbport
            self.__gdb_dbname = dbname
            self.connected = 1
            self.gnnode_table = "gnnodes"
            self.gnbizrules_table = "gnbizrules"
            self.__gdb_metadb_schema = "gnmeta"
            self.__gdb_metanodes_tbl = "gnnodes"
            self.__gdb_metaedges_tbl = "gnedges"
            #self.dbtype = dbtype
            
        except exc.SQLAlchemyError as err:
            self.dbConnp = None
            self.connected = 0
            gn_log('GnIngPgresDBOps: unable to connect pgres DB Error ')
            gn_log(err)
            
        except exc.OperationalError as err:
            self.dbConnp = None
            self.connected = 0
            gn_log('GnIngPgresDBOps: unable to connect pgres Operational Error ')
            gn_log(err)
                                                            
      
        
    def db_query_execute(self, conn, query):
        try:
                    result = conn.execute(query)
        except sqlalchemy.exc.OperationalError:  # may need more exceptions here (or trap all)
                    conn = engine.connect()  # replace your connection
                    result = conn.execute(query)  # and retry
        return result


    def   db_create_table(self, tabl_sql_str):
       #create table
       try:
           
            self.dbEngine.execute(tabl_sql_str)
            ###self.dbEngine.commit()
            return 0
       except exc.SQLAlchemyError as err:
            print('GnIngPgresDBOps: Error creating table ')
            print(err)
            return -1
    
    def  df_to_sql(self, engine, df, table, schema, if_exists='fail', sep='\t', encoding='utf8'):
        
        # Create Table
        #df[:0].to_sql(table, engine, if_exists=if_exists)

        # Prepare data
        output = StringIO()
        c = df.count()
        print(c)
        df.to_csv(output, sep=sep, header=False, encoding=encoding)
        output.seek(0)
        
        # Insert data
        connection = engine.raw_connection()
        cursor = connection.cursor()

        ###cursor.execute("SELECT * from customer.customer")

        ###data = cursor.fetchall()
        ###print(data)
        t = schema+'."'+table+'"'
        ##print(' Copying data to '+t)
        cursor.copy_from(output, t,  sep=sep, null='')
        connection.commit()
        cursor.close()
    
    def  grphdb_create_table(self, nodename, schema):
        gn_log('GnIngPgresDBOps: create table for node '+nodename)
        tbl_str = schema+'.'+'"'+nodename+'"'
        
        dnode_tablestr = 'CREATE TABLE IF NOT EXISTS '+tbl_str+' (gnnodeid bigint NOT NULL PRIMARY KEY, gnnodetype text, gnmetanodeid bigint,  gndatanodeprop json,  gndatanodeobj json, uptmstmp timestamp);'
        gn_log("GnGrphDBOps:  table_str "+dnode_tablestr)
        self.db_create_table(dnode_tablestr)
        gn_log("GnIngPgresDBOps: table "+tbl_str+" is created ");
       
        
    def    _isconnected(self):
        if (self.dbConnp is None):
            return 0
        else:
            return 1

    def    metadb_nodes_getall(self, isResultDataFrame):
        psql_query = '''
             SELECT  * FROM gnmeta.gnnodes
             ''';

        resDF = pds.read_sql(psql_query, self.dbConnp)

        if isResultDataFrame:
            return resDF
        else:
            return resDF.values.tolist()

    def    metadb_nodes_get_metanodes(self, isResultDataFrame):
        psql_query = '''
             SELECT  * FROM gnmeta.gnnodes where gnnodetype='GNMetaNode'
             ''';

        resDF = pds.read_sql(psql_query, self.dbConnp)

        if isResultDataFrame:
            return resDF
        else:
            return resDF.values.tolist()
        


    def    metadb_nodes_write(self, metaDF):
        ### Write metaDF to db
        ### insert mdf to postgresdb
        #tgt_table="gnnodes"
        #tgt_schema= "gnmeta"

        ##if (self.dbtype != "gnmetadb"):
        ##    return -1
        
        if (self.connected):
            metaDF.to_sql(self.gnnode_table, self.dbConnp, schema=self.__gdb_metadb_schema,  if_exists='append', index=False)
            return 0

    def  metadb_edges_write(self, metaedgeDF):
        ### Write metaDF to db
        ### insert mdf to postgresdb
        #tgt_table="gnedges"
        #tgt_schema= "gnmeta"
        ##if (self.dbtype != "gnmetadb"):
        ##    return -1
        
        if (self.connected):
            metaedgeDF.to_sql(self.__gdb_metaedges_tbl, self.dbConnp, schema=self.__gdb_metadb_schema,  if_exists='append', index=False)



    def  datadb_nodes_write(self, dataDF, tgt_schema, tgt_table):

        gn_log("GnIngPgresDBOps: copying datanode "+tgt_schema+"."+tgt_table+" to db ")
        
        if (self.connected):        
            gn_log("GnIngPgresDBOps: copying datanode for "+tgt_schema+"."+tgt_table+"  to db "+self.__gdb_dbname)
            try:
                n = dataDF.to_sql(tgt_table, con=self.dbConnp, schema=tgt_schema,  if_exists='append', index=False, chunksize=5000, method='multi')
            except psycopg2.DatabaseError as e:
                gn_log("GnIngPgresDBOps: database write error ")
                gn_log(e)
                return -1
            return 0
        else:
            gn_log("GnIngPgressDBOps: database is not connected. Failing the operation ")
            return -1

            
    def  datadb_edges_write_na(self, dataEdgeDF):
        ### Write metaDF to db
        ### insert mdf to postgresdb
        #tgt_table="gnedges"
        #tgt_schema= "gnmeta"
        
        if (self.connected):
            dataEdgeDF.to_sql(self.__gdb_metaedges_tbl, self.dbConnp, schema=self.gnmeta_schema,  if_exists='append', index=False)


            
            
    def get_metanode_id(self, name):

        if (self.dbtype != "gnmetadb"):
            return -1
        
        if (self.connected):
            sql = f"SELECT gnnodeid from gnmeta.gnnodes where gnnodename='"+name+"';"
            r = self.dbEngine(sql)
            gn_node_id = r[0][0]
            return gn_node_id
        else:
            return -1


    def   create_gndata_datatable_obsolete(self, bizdomain, nodename):

        ##if (self.dbtype != "gndatadb"):
        ##    return -1
        tabl_name = bizdomain.lower()+"."+nodename.lower()
        datatbl_col= "gnnodeid bigint NOT NULL PRIMARY KEY, gnnodetype text, gnmetanodeid bigint, gndatanodeprop json, gndatanodeobj json, uptmstmp Timestamp"  
        create_tbl_str = "CREATE TABLE IF NOT EXISTS "+tabl_name+" ("+datatbl_col+")"
        print('gnPgreDBOps: tablstr: '+create_tbl_str)
        self.dbEngine.execute(create_tbl_str) 
        return


    def      metadb_metanode_chk_nodeid(self, nodename):

        psql_query = "SELECT * from gnmeta.gnnodes where gnnodename='"+nodename+"' AND gnnodetype='GNMetaNode' "
        resDF = pds.read_sql(psql_query, self.dbConnp)
        nodeid=None
        
        if (resDF.shape[0] > 0):
            for x in resDF["gnnodeid"]:
                nodeid = x
        else:
            nodeid = -1
        ##return resDF.values.tolist()
        return nodeid


    def      metadb_bizrules_rule_chk(self, srcnodeid, rulename):

        gnbizr_tbl = self.gnmeta_schema+"."+self.gnbizrules_table
        psql_query = "SELECT * from "+gnbizr_tbl+" where gnrelname='"+rulename+"' AND gnsrcnodeid="+str(srcnodeid)+"  "
        resDF = pds.read_sql(psql_query, self.dbConnp)
        relid=-1
        if (resDF.shape[0] > 0):
            for x in resDF["gnrelid"]:
                relid = x
        else:
            relid = -1

        return relid



    def      metadb_bizrules_bizr_get(self, bizrid):

        gnbizr_tbl = self.gnmeta_schema+"."+self.gnbizrules_table
        psql_query = "SELECT * from "+gnbizr_tbl+"  where gnrelid="+str(bizrid)+"   "
        resDF = pds.read_sql(psql_query, self.dbConnp)
        res = resDF.to_json(orient="records")
        rJson = json.loads(res)
         
        return rJson



    
    
    def  metadb_bizrules_write(self, metaBizRuleDF):

        if (self.connected):
            metaBizRuleDF.to_sql(self.gnbizrules_table, self.dbConnp, schema=self.gnmeta_schema,  if_exists='append', index=False)
            return 0
    
