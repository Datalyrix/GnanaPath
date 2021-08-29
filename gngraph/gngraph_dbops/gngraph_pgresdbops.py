import psycopg2
from sqlalchemy import create_engine
import pandas as pds

"""
gngraph db implementation main class and associated functions
current implementation uses postgres as underlying database

"""


class GNGraphPgresDBOps:

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
            self.gnmeta_schema = "gnmeta"
            self.gnedge_table = "gnedges"
            self.dbtype = dbtype
    
        except:
            self.dbConnp = None
            self.connected = 0
            print('gngraphPgresDBOps: unable to connect pgres ')

    def  metadb_nodes_getall(self, isResultDataFrame):
        psql_query = '''
             SELECT  * FROM gnmeta.gnnodes
             ''';

        resDF = pds.read_sql(psql_query, self.dbConnp)

        if isResultDataFrame:
            return resDF
        else:
            return resDF.values.tolist()



    def  metadb_nodes_write(self, metaDF):
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
