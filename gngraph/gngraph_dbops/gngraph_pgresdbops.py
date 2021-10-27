import psycopg2
from sqlalchemy import create_engine, exc
import json
import pandas as pds

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
            print('PgresDBOps: Init ')
            print(pgres_connstr)
            alchemyEngine   = create_engine(pgres_connstr, pool_recycle=3600)
            # Connect to PostgreSQL server
            self.dbEngine = alchemyEngine
            self.dbConnp = alchemyEngine.connect();
            self.dbuser = dbuser
            self.dbpasswd = dbpasswd
            self.dbserver = dbserver
            self.dbport = dbport
            self.dbname = dbname
            self.connected = 1
            self.gnnode_table = "gnnodes"
            self.gnbizrules_table = "gnbizrules"
            self.gnmeta_schema = "gnmeta"
            self.gnedge_table = "gnedges"
            self.dbtype = dbtype
            print('pgresDBOps: Connected  ')
            print(self.dbConnp)
        except exc.SQLAlchemyError as err:
            self.dbConnp = None
            self.connected = 0
            print('gngraphPgresDBOps: unable to connect pgres ')
            print(err)
            
        except exc.OperationalErro as err:
            self.dbConnp = None
            self.connected = 0
            print('gngraphPgresDBOps: unable to connect pgres ')
            print(err)
                                                            
      
        
    def db_query_execute(self, conn, query):
        try:
                    result = conn.execute(query)
        except sqlalchemy.exc.OperationalError:  # may need more exceptions here (or trap all)
                    conn = engine.connect()  # replace your connection
                    result = conn.execute(query)  # and retry
        return result
                            
            
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



    def    metadb_nodes_write(self, metaDF):
        ### Write metaDF to db
        ### insert mdf to postgresdb
        #tgt_table="gnnodes"
        #tgt_schema= "gnmeta"

        if (self.dbtype != "gnmetadb"):
            return -1
        
        if (self.connected):
            metaDF.to_sql(self.gnnode_table, self.dbConnp, schema=self.gnmeta_schema,  if_exists='append', index=False)
            return 0

    def  metadb_edges_write(self, metaedgeDF):
        ### Write metaDF to db
        ### insert mdf to postgresdb
        #tgt_table="gnedges"
        #tgt_schema= "gnmeta"
        if (self.dbtype != "gnmetadb"):
            return -1
        
        if (self.connected):
            metaedgeDF.to_sql(self.gnedge_table, self.dbConnp, schema=self.gnmeta_schema,  if_exists='append', index=False)



    def  datadb_nodes_write(self, dataDF, tgt_schema, tgt_table):
        ### Write metaDF to db
        ### insert mdf to postgresdb
        #tgt_table="gnnodes"
        #tgt_schema= "gnmeta"

        if (self.dbtype != "gndatadb"):
            return -1
        print("gnPgresDBops: Writing data node for "+tgt_schema+"."+tgt_table+"  ")
        if (self.connected):
            dataDF.to_sql(tgt_table, self.dbConnp, schema=tgt_schema,  if_exists='append', index=False)
            return 0



            
    def  datadb_edges_write_na(self, dataEdgeDF):
        ### Write metaDF to db
        ### insert mdf to postgresdb
        #tgt_table="gnedges"
        #tgt_schema= "gnmeta"
        
        if (self.connected):
            dataEdgeDF.to_sql(self.gnedge_table, self.dbConnp, schema=self.gnmeta_schema,  if_exists='append', index=False)


            
            
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


    def   create_gndata_datatable(self, bizdomain, nodename):

        if (self.dbtype != "gndatadb"):
            return -1
        tabl_name = bizdomain+"."+nodename
        datatbl_col= "gnnodeid bigint NOT NULL PRIMARY KEY, gnnodetype text, gnmetanodeid bigint, gndatanodeprop json, gndatanodeobj json, uptmstmp Timestamp"  
        create_tbl_str = "CREATE TABLE IF NOT EXISTS "+tabl_name+" ("+datatbl_col+")"
        print('PgreDBOps: tablstr: '+create_tbl_str)
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

