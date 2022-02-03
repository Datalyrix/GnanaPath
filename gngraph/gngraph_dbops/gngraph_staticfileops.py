import os
from os import path
import pandas as pds
import json
import pathlib
from gnutils.gn_log import gn_log, gn_log_err

"""
gngraph static fileops implementation main class and associated functions
current implementation uses gndata gngraph folder

"""


class  GNGraphStaticFileOps:


    def __init__(self, gndata_graph_folder):
            self.gndata_graph_folder = gndata_graph_folder
            self.gndata_graph_data_folder = gndata_graph_folder+"/data"
            self.gndata_graph_config_folder = gndata_graph_folder+"/config"
            self.gnnode_table = "gnnodes"
            self.gnmeta_schema = "gnmeta"
            self.gnedge_table = "gnedges"
            self.gnmetanode_filepath = self.gndata_graph_data_folder+"/"+"gnmetanodes.json"
            self.gnmetaedge_filepath = self.gndata_graph_data_folder+"/"+"gnmetaedges.json"
            self.gnmetabizrules_filepath = self.gndata_graph_data_folder+"/"+"gnmetabizrules.json"  

    def  metadb_load_metanode_df(self):

        if path.exists(self.gnmetanode_filepath):
            jDF = pds.read_json(self.gnmetanode_filepath)
        else:
            jDF = None
            
        return jDF
    
    def  metadb_load_metaedge_df(self):
        if path.exists(self.gnmetaedge_filepath):
            jDF = pds.read_json(self.gnmetaedge_filepath)
        else:
            jDF = None
        return jDF

    
    def  metadb_nodes_append_write(self, metaDF):

        if path.exists(self.gnmetanode_filepath):
            cDF = pds.read_json(self.gnmetanode_filepath)
            jDF = cDF.append(metaDF, ignore_index=True)
        else:
            jDF = metaDF
            
        jstr = jDF.to_json(orient='records')
        jdict = jDF.to_dict(orient='records')

        with open(self.gnmetanode_filepath, 'w') as fp:
            json.dump(jdict, fp)

    def  metadb_nodes_overwrite_write(self, metaDF):

        jDF = metaDF
        jstr = jDF.to_json(orient='records')
        jdict = jDF.to_dict(orient='records')

        with open(self.gnmetanode_filepath, 'w') as fp:
            json.dump(jdict, fp)


            
    def  metadb_edges_append_write(self, metaedgeDF):
        ### Write metaDF to db
        ### insert mdf to postgresdb
        #tgt_table="gnedges"
        #tgt_schema= "gnmeta"

        if path.exists(self.gnmetaedge_filepath):
            cDF = pds.read_json(self.gnmetaedge_filepath)
            jDF = cDF.append(metaedgeDF, ignore_index=True)
        else:
            jDF = metaedgeDF

        jstr = jDF.to_json(orient='records')
        jdict = jDF.to_dict(orient='records')

        with open(self.gnmetaedge_filepath, 'w') as fp:
            json.dump(jdict, fp)

            
    def  metadb_edges_overwrite_write(self, metaedgeDF):
        jDF = metaedgeDF
        jstr = jDF.to_json(orient='records')
        jdict = jDF.to_dict(orient='records')

        with open(self.gnmetaedge_filepath, 'w') as fp:
            json.dump(jdict, fp)

            
    def  metadb_metanode_id_get(self, name):

          metaDF = pds.read_json(self.gnmetanode_filepath)
          metaDF.head(2)
          gn_log("GnGrphSfOps: getting metanode: "+self.gnmetanode_filepath)
          
          rDF = metaDF.query('gnnodename == "'+name+'"')

          if (rDF.shape[0] > 0):
              for x in rDF["gnnodeid"]:
                  gnnode_id = x
          else:
              gnnode_id = -1
          return gnnode_id

      
    def   create_gndata_datadirs(self, bizdomain, nodename):

         bizdomain_dir = self.gndata_graph_data_folder+"/"+bizdomain
         if not os.path.exists(bizdomain_dir):
             os.makedirs(bizdomain_dir)

         dnode_dir = bizdomain_dir+"/"+nodename
         if not os.path.exists(dnode_dir):
             os.makedirs(dnode_dir)

    def  datadb_load_metanode_df(self, bizdomain, nodename):

        bizdomain_dir = self.gndata_graph_data_folder+"/"+bizdomain

        if not os.path.exists(bizdomain_dir):
            gn_log("GnGrphSfOps: biz domain "+bizdomain+" directory is not present")
            return None

        dnode_dir = bizdomain_dir+"/"+nodename
        dnode_file = dnode_dir+"/"+nodename+".json"

        gn_log("GnGrphSfOps: reading datanode file "+dnode_file)
        if path.exists(dnode_file):
            jDF = pds.read_json(dnode_file)
        else:
            jDF = None
            
        return jDF
             

    def  datadb_nodes_write(self, dataNodeDF, bizdomain, nodename):

        bizdomain_dir = self.gndata_graph_data_folder+"/"+bizdomain
        dnode_dir = bizdomain_dir+"/"+nodename
        dnode_file = dnode_dir+"/"+nodename+".json"
        
        jstr = dataNodeDF.to_json(orient='records')
        jdict = dataNodeDF.to_dict(orient='records')

        with open(dnode_file, 'w') as fp:
            json.dump(jdict, fp)



    def  datadb_edges_write(self, dataEdgeDF):
        ### Write metaDF to db
        ### insert mdf to postgresdb
        #tgt_table="gnedges"
        #tgt_schema= "gnmeta"
        jstr = metaedgeDF.to_json(orient='records')
        jdict = metaedgeDF.to_dict(orient='records')

        with open(self.gnmetaedge_filepath, 'w') as fp:
            json.dump(jdict, fp)


    def  metadb_bizrules_rule_chk(self, srcnodeid, rulename):

        relid = -1
        if path.exists(self.gnmetabizrules_filepath):
        
           metaBizRDF = pds.read_json(self.gnmetabizrules_filepath)
           rDF = metaBizRDF.query("gnsrcnodeid == "+str(srcnodeid)+' and  gnrelname == "'+rulename+'" ')
           relid = -1
           ###nres = rDF["gnnodeid"].count()
           if (rDF.shape[0] > 0):
              for x in rDF["gnrelid"]:
                  relid = x
           else:
               relid = -1

        return relid


    def      metadb_bizrules_bizr_get(self, bizrid):

        rJson = {}
        if path.exists(self.gnmetabizrules_filepath):
           metaBizRDF = pds.read_json(self.gnmetabizrules_filepath)
           rDF = metaBizRDF.query("gnrelid == "+str(bizrid)+" ")
           res = resDF.to_json(orient="records")
           rJson = json.loads(res)

        return rJson


    
            
    def  metadb_bizrules_append_write(self, metaDF):


        if path.exists(self.gnmetabizrules_filepath):
            cDF = pds.read_json(self.gnmetabizrules_filepath)
            jDF = cDF.append(metaDF, ignore_index=True)
        else:
            jDF = metaDF

        jstr = jDF.to_json(orient='records')
        jdict = jDF.to_dict(orient='records')

        with open(self.gnmetabizrules_filepath, 'w') as fp:
            json.dump(jdict, fp)
