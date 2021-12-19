import os
import sys
import psycopg2
import pandas as pds
import json
from sqlalchemy import create_engine
"""
    GnGraph Ingest Ops for batch files using pd frames

"""

curentDir = os.getcwd()
parentDir = curentDir.rsplit('/', 1)[0]
if parentDir not in sys.path:
    sys.path.append(parentDir)

from gngraph.config.gngraph_config import GNGraphConfig
from gngraph.gngraph_dbops.gngraph_pgresdbops import GNGraphPgresDBOps
from gngraph.gngraph_dbops.gngraph_staticfileops import GNGraphStaticFileOps
from gnutils.gn_log import gn_log, gn_log_err


class     GNGraphIngestOps:

    def __init__(self, fileargs, gdbargs):

        self.__fargs = fileargs
        self.__gdbargs = gdbargs
        if (self.__fargs["ftype"] == "csv"):
            #try:
            self.__nodeDF = pds.read_csv(self.__fargs["fpath"], delimiter=self.__fargs["fdelim"])
            #except Exception as err:
            #    print(err)
            gn_log('GNIngest: file is parsed and created dataframe')

        ### Establish metadb conn and datadb conn    
        if (self.__gdbargs["gdbflag"]):
            self.gdb_conn_setup()

        if (self.__gdbargs["staticfiles"]):
            gngraph_folder = self.__gdbargs["gndatafolder"]+"/gngraph";
            self.__gngrp_sfops = GNGraphStaticFileOps(gngraph_folder)
            
        ## get config class
        if (self.__gdbargs["gndatafolder"]):
            id_cfg_path = self.__gdbargs["gndatafolder"]+"/gngraph/config"
            self.__gngrp_cfg = GNGraphConfig(id_cfg_path)

        ###set metanode columns for pgresdb and static files
        self.__metanode_columns=["gnnodeid", "gnnodename", "gnnodetype", "gnnodeprop", "uptmstmp"]
        self.__metaedge_columns=["gnedgeid", "gnedgename", "gnedgetype", "gnsrcnodeid", "gntgtnodeid", "gnedgeprop", "uptmstmp"]

        self.gnnodeparentid = -1
        self.gnnode_parent_name = self.__fargs["nodename"]
            
    def    gdb_conn_setup(self):

        ###with open(self.__gdbargs["gdbcredsfpath"], encoding="utf-8") as fh:
        ###     gdb_creds = json.load(fh)
        gdb_creds = gn_pgresdb_getconfiguration(self.__gdbargs["gdbcredsfpath"])        
        self.__gdbMetaDBConnp = GNGraphPgresDBOps.from_args(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"], self.__gdbargs["gnmetaDB"], "gnmetadb")         
        self.__gdbDataDBConnp = GNGraphPgresDBOps.from_args(gdb_creds["dbserver"], gdb_creds["dbport"], gdb_creds["dbuser"], gdb_creds["dbpasswd"],  self.__gdbargs["gndataDB"], "gndatadb")

    def     get_metanodeid_byname(name):
        if (self.__gdbargs["gdbflag"]):
            gn_node_id = self.__gdbMetaDBConnp.get_metanode_id(name)
            return gn_node_id
        if (self.__gdbargs["staticfiles"]):
            gn_node_id = self.__gngrp_sfops.get_metanode_id(name)
            return gn_node_id
        
    def    get_metanode_parent_id(self):

        return self.gnnodeparentid
        
    def    create_node_metanodes_edges(self):
        gn_log("GNGraphIngestOps: "+self.__fargs["nodename"]+" creating meta nodes and edges ")
        ##### Add new metanode and metanode attributes to metaDF
        metanodeprop = {"gnlabel": self.__fargs["nodename"], "bizdomain": self.__fargs["bizdomain"]}
        metanodepropstr = json.dumps(metanodeprop)
        utmstmp = pds.Timestamp.now()
        gn_nodeid_max_c = self.__gngrp_cfg.get_nodeid_max()+1
        metanode_e = [gn_nodeid_max_c, self.__fargs["nodename"], "GNMetaNode", metanodepropstr, utmstmp]
        
        gn_log(metanode_e)
        #metaDF.loc[len(metaDF.index)] = metanode_e
        #metaDF.head()
        self.__metanodeDF = pds.DataFrame([metanode_e], columns=self.__metanode_columns)
        ### save metanode
        ##self.__gngrp_cfg.save_nodeid_max(gn_nodeid_max_c)
        #### Now update other meta node attributes (columns)
        self.gnnodeparentid = gn_nodeid_max_c
        self.gnnode_parent_name = self.__fargs["nodename"]
        nodedf_collist = self.__nodeDF.columns
        utmstmp = pds.Timestamp.now()
        attr_rel_name="HAS_ATTR"
        attr_edge_type="GNMetaNodeEdge"
        mnodeattr_arr=[]
        medgeattr_arr=[]
        nodename = self.__fargs["nodename"]
        #### Let us get latest id
        gn_nodeid_c = gn_nodeid_max_c
        gn_edgeid_c = self.__gngrp_cfg.get_edgeid_max()
        
        for c in nodedf_collist:
            gn_nodeid_c=gn_nodeid_c+1
            gn_edgeid_c=gn_edgeid_c+1
            metanodeprop = {"gnlabel":c, "gnnodeparent":nodename, "bizdomain": self.__fargs["bizdomain"]}
            metaedgeprop = {"gntgtlabel":c, "gnsrclabel": nodename, "gnsrcnodeloc": "gnnodes", "gnsrcdomain": "gnmeta", "gntgtnodeloc": "gnnodes", "gntgtdomain":"gnmeta"}
            metanodepropstr = json.dumps(metanodeprop)
            metaedgepropstr = json.dumps(metaedgeprop)
            metanode_e = [gn_nodeid_c, c, "GNMetaNodeAttr", metanodepropstr, utmstmp]
            metaedge_e = [gn_edgeid_c, attr_rel_name, attr_edge_type, self.gnnodeparentid, gn_nodeid_c, metaedgepropstr, utmstmp]
            mnodeattr_arr.append(metanode_e)
            medgeattr_arr.append(metaedge_e)

        metaColDF = pds.DataFrame(mnodeattr_arr, columns=self.__metanode_columns)
        ###metaColDF.head()
        self.__metanodeDF = self.__metanodeDF.append(metaColDF, ignore_index=True)
        self.__metaedgeDF = pds.DataFrame(medgeattr_arr, columns=self.__metaedge_columns)

        ### Now write metanodes and edges to db and static files
        if (self.__gdbargs["gdbflag"]):
           self.__gdbMetaDBConnp.metadb_nodes_write(self.__metanodeDF)
           self.__gdbMetaDBConnp.metadb_edges_write(self.__metaedgeDF)

        if (self.__gdbargs["staticfiles"]):
           print("gnGraphIngest: write nodes and edges to static files ") 
           self.__metanodeDF["uptmstmp"] = self.__metanodeDF["uptmstmp"].astype(str)
           self.__metaedgeDF["uptmstmp"] = self.__metaedgeDF["uptmstmp"].astype(str)
           self.__gngrp_sfops.metadb_nodes_append_write(self.__metanodeDF)
           self.__gngrp_sfops.metadb_edges_append_write(self.__metaedgeDF)
                       
        #### Update nodeidmax and edgeidmax
        self.__gngrp_cfg.save_nodeid_max(gn_nodeid_c)
        self.__gngrp_cfg.save_edgeid_max(gn_edgeid_c)
        gn_log("GNGraphIngestOps: "+self.__fargs["nodename"]+" meta nodes and edges created successfully ")
         
    def   create_node_datanodes_edges(self):

         gn_log("GNGraphIngestOps: "+self.__fargs["nodename"]+" creating data nodes and edges ")
         gn_node_parent_id = self.get_metanode_parent_id()
         gn_nodeid_max_c = self.__gngrp_cfg.get_nodeid_max()
         gn_edgeid_max_c = self.__gngrp_cfg.get_edgeid_max()

         jstr = self.__nodeDF.to_json(orient='records')
         jobj = json.loads(jstr)

         #convert json elements to strings and then load to df.
         jDF = pds.DataFrame([json.dumps(e) for e in jobj], columns=["gndatanodeobj"])

         self.__nodeDF["gndatanodeobj"] = [json.dumps(e) for e in jobj]
         gn_nodeid_c = gn_nodeid_max_c+1
         gn_edgeid_c = gn_edgeid_max_c+1
         
         self.__nodeDF['gnnodeid'] = pds.RangeIndex(stop=self.__nodeDF.shape[0])+gn_nodeid_c
         self.__nodeDF['gnnodetype']="GNDataNode"
         self.__nodeDF['gnmetanodeid']= gn_node_parent_id
         gn_nodeid_max_n = self.__nodeDF.shape[0]+gn_nodeid_c-1
         self.__nodeDF["gndatanodeprop"] =  self.__nodeDF["gnnodeid"].apply(lambda x: json.dumps({'gnlabel':self.gnnode_parent_name+str(x)}))

         #### Update utimestamp
         utmstmp = pds.Timestamp.now()
         self.__nodeDF['uptmstmp'] = utmstmp
         
         self.__nodeEdgeDF = self.__nodeDF.copy()
         
         self.__gndatanodeDF = self.__nodeDF[["gnnodeid","gnnodetype","gnmetanodeid","gndatanodeprop","gndatanodeobj", "uptmstmp"]]
         ###self.__gnmdatanodeDF = self.__gndatanodeDF.select("gnnodeid", "gnnodetype", "gndatanodeprop", "uptmstmp")
         self.__gnmdatanodeDF = self.__gndatanodeDF
         
         ##self.__gnmdatanodeDF
         
         if (self.__gdbargs["gdbflag"]):
             self.__gdbDataDBConnp.create_gndata_datatable(self.__fargs["bizdomain"], self.__fargs["nodename"])
         
         if (self.__gdbargs["staticfiles"]):
             self.__gngrp_sfops.create_gndata_datadirs(self.__fargs["bizdomain"], self.__fargs["nodename"])

         ### Now write datanodes to domain schema table
         if (self.__gdbargs["gdbflag"]):
             self.__gdbDataDBConnp.datadb_nodes_write(self.__gndatanodeDF, self.__fargs["bizdomain"], self.__fargs["nodename"])

         if (self.__gdbargs["staticfiles"]):
             # for static files timestamp has to be JSON serializable
             self.__gndatanodeDF["uptmstmp"] = self.__gndatanodeDF["uptmstmp"].astype(str)
             self.__gngrp_sfops.datadb_nodes_write(self.__gndatanodeDF, self.__fargs["bizdomain"], self.__fargs["nodename"])
             
         ### save node id
         self.__gngrp_cfg.save_nodeid_max(gn_nodeid_max_n)

         ### Copy datanodes to gnnodes
         cdf=pds.io.json.json_normalize(self.__gnmdatanodeDF["gndatanodeprop"].apply(json.loads).apply(lambda x: x))
         self.__gnmdatanodeDF["gnnodename"]=cdf['gnlabel']

         ### set gnnodeprop
         self.__gnmdatanodeDF["gnnodeprop"] = self.__gnmdatanodeDF["gnnodeid"].apply(lambda x: json.dumps({"gnnodeparent":self.gnnode_parent_name, "bizdomain": self.__fargs["bizdomain"]}))
         
         #####self.__gnmdatanodeDF.rename(columns=({'gnnode' 
         self.__gnMetaDatanodeDF = self.__gnmdatanodeDF[["gnnodeid", "gnnodename", "gnnodetype", "gnnodeprop", "uptmstmp"]]

         
         ###################### Write datanodes to metatable
         if (self.__gdbargs["gdbflag"]):
             self.__gdbMetaDBConnp.metadb_nodes_write(self.__gnMetaDatanodeDF)
         if (self.__gdbargs["staticfiles"]):
             self.__gngrp_sfops.metadb_nodes_append_write(self.__gnMetaDatanodeDF)
             

         ## Rename column gnmetanodeid to gnsrcnodeid
         self.__nodeEdgeDF.rename( columns=({'gnnodeid':'gntgtnodeid', 'gnmetanodeid':'gnsrcnodeid'}), inplace=True)

         ## Add gnedgeid
         self.__nodeEdgeDF['gnedgeid'] = pds.RangeIndex(stop=self.__nodeEdgeDF.shape[0])+gn_edgeid_c

         #### Add gnedgename IS
         relname="IS"
         self.__nodeEdgeDF["gnedgename"] = relname   

         ### Add gnedgetype
         edgetype="GNDataNodeEdge"
         self.__nodeEdgeDF["gnedgetype"] = edgetype

         #### Add gnedgeprop
         self.__nodeEdgeDF["gnedgeprop"] = self.__nodeEdgeDF["gntgtnodeid"].apply(lambda x: json.dumps({'gntgtlabel':self.gnnode_parent_name+str(x), 'gnsrclabel':self.gnnode_parent_name, 'gnsrcnodeloc': "gnnodes", "gnsrcdomain": "gnmeta", "gntgtnodeloc":   self.__fargs["nodename"], "gntgtdomain": self.__fargs["bizdomain"] }))

         #### Update utimestamp
         utmstmp = pds.Timestamp.now()
         self.__nodeEdgeDF['uptmstmp'] = utmstmp

         #####Select edgenode columns and prepare for write
         self.__gndatanodeEdgeDF = self.__nodeEdgeDF[["gnedgeid","gnedgename","gnedgetype","gnsrcnodeid","gntgtnodeid","gnedgeprop", "uptmstmp"]]
         ## write edges to database
         if (self.__gdbargs["gdbflag"]):
             self.__gdbMetaDBConnp.metadb_edges_write(self.__gndatanodeEdgeDF)
         if (self.__gdbargs["staticfiles"]):
             self.__gndatanodeEdgeDF["uptmstmp"] = self.__gndatanodeEdgeDF["uptmstmp"].astype(str)
             self.__gngrp_sfops.metadb_edges_append_write(self.__gndatanodeEdgeDF)
             
         #save edgeid max
         gn_edgeid_max_n = self.__gndatanodeEdgeDF.shape[0]+gn_edgeid_c-1
         self.__gngrp_cfg.save_edgeid_max(gn_edgeid_max_n)
         
         gn_log("GNGraphIngestOps: "+self.__fargs["nodename"]+" data nodes and edges created succesfully")
         

def     gngraph_ingest_file_api(filename, ftype, fdelim, nodename, bizdomain, gndata_folder, gngraph_creds_folder, gncfg_settings):


    gdb_creds_filepath=gngraph_creds_folder+"/gngraph_pgres_dbcreds.json"
    fileargs = {}
    fileargs["fpath"] = gndata_folder+"/uploads/"+filename
    fileargs["fname"] = filename
    fileargs["nodename"] = nodename
    fileargs["ftype"] = ftype
    fileargs["fdelim"] = fdelim
    fileargs["bizdomain"] = bizdomain

    print(' gncfg settings ')
    print(gncfg_settings)
    print('file args ')
    print(fileargs)
    gdbargs = {}
    gdbargs["gdb"] = "pgres"
    gdbargs["gdbflag"] = gncfg_settings["dbmode"]
    gdbargs["gdbcredsfpath"] = gdb_creds_filepath
    gdbargs["gnmetaDB"] = "gngraph_db"
    gdbargs["gndataDB"] = "gngraph_db"
    gdbargs["staticfiles"] = 1
    gdbargs["staticfpath"] = gndata_folder+"/uploads";
    gdbargs["gndatafolder"] = gndata_folder
        
    gnIngestp = GNGraphIngestOps(fileargs, gdbargs)

    ## First create metanodes and metaedges 
    gnIngestp.create_node_metanodes_edges()

    ## Create Datanodes and edges
    gnIngestp.create_node_datanodes_edges()

    


    
if __name__ == "__main__":
    
    filename="salesorder.csv"
    ftype="csv"
    fdelim=','
    nodename="salesorder"
    bizdomain="sales_domain"
    gndata_folder="/home/jovyan/GnanaDiscover/GnanaPath/gndata"
    gngraph_creds_folder = "/home/jovyan/GnanaDiscover/GnanaPath/creds/gngraph"
    gngraph_ingest_file_api(filename, ftype, fdelim, nodename, bizdomain, gndata_folder, gngraph_creds_folder)



    
