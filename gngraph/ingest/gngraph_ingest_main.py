# Prepare the configuration file to be used with spark
# Get the upload path and the filename 
# TODO: Get the filetype and name once the user uploads
import os
from os import path
import sys
import json
import pathlib
import pickle
import numpy as np
import chardet
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit,split
from pyspark.sql import Row
from pyspark.sql.types import *



class     gnGraphIngest:

    gncfg = {
            'app_name': 'gnana-graph',
            'gngraph_root_dir':'',
            'gngraph_uploads_dir':'',
            'gngraph_cfg_dir': '',
            'gngraph_cfg_filename': '',
            'gngraph_cfg_nodeid_filename': '',
            'gngraph_cfg_edgeid_filename': '',
            'gngraph_data_dir': '', 
            'gngraph_metanode_filename': '',
            'gngraph_metanodeattr_filename': '',
            'gngraph_edge_filename': '',
     }
    def __init__(self, rtDir):
        ###Set up config init routine
        self.gngraph_config_init(rtDir)
        
        


    def   gngraph_config_init(self, rtDir):
       # check if the config_file exists 
       gngraph_path = rtDir+"/gndata/gngraph"
       self.gncfg["gngraph_root_dir"] = gngraph_path
       self.gncfg["gngraph_uploads_dir"] = rtDir+"/gndata/uploads"
       self.gncfg["gngraph_cfg_dir"] = gngraph_path+"/config"
       self.gncfg["gngraph_data_dir"] = gngraph_path+"/data"
       self.gncfg["gngraph_cfg_filename"] = "gngraph_config.json"
       self.gncfg["gngraph_cfg_nodeid_filename"] = "gnnode_id.pkl"
       self.gncfg["gngraph_cfg_edgeid_filename"] = "gnedge_id.pkl"
       self.gncfg["gngraph_metanode_filename"] = "gnmetanodes.json"
       self.gncfg["gngraph_metanodeattr_filename"] = "gnmetanodeattrs.json"
       self.gncfg["gngraph_edge_filename"] = "gnmetaedges.json"
       self.gncfg["gngraph_node_filename"] = "gnmetanodes.json"
       self.spark = None
       
       self.meta_edge_filepath = self.gncfg["gngraph_data_dir"]+"/"+self.gncfg["gngraph_edge_filename"] 
    
       self.meta_node_filepath = self.gncfg["gngraph_data_dir"]+"/"+self.gncfg["gngraph_node_filename"]
  
    
       if not os.path.isdir(self.gncfg["gngraph_root_dir"]):
           os.mkdir(self.gncfg["gngraph_root_dir"])
    
       if not os.path.isdir(self.gncfg["gngraph_cfg_dir"]):
           os.mkdir(self.gncfg["gngraph_cfg_dir"])
        
       if not os.path.isdir(self.gncfg["gngraph_data_dir"]):
           os.mkdir(self.gncfg["gngraph_data_dir"]) 
 
          
       self.gngraph_cfgdata_write()  
       ### 
       self.gngraph_nodeid_init()
       self.gngraph_edgeid_init()


    def   gngraph_cfgdata_write(self):
       cfgfpath = self.gncfg["gngraph_cfg_dir"]+"/"+self.gncfg["gngraph_cfg_filename"]
       print('gngraph_ingest: gngraph_config init:'+cfgfpath) 
       with open(cfgfpath,"w") as out_file:
           json.dump(self.gncfg, out_file)

    # If config file already exists read the data
    def    gngraph_cfgdata_read(self):
        config_file = self.gncfg["gngraph_cfg_dir"]+"/"+self.gncfg["gngraph_cfg_filename"]
        print('gngraph_ingest: gngraph_config init:'+cfgfpath) 
        with open(path,'r') as config_file:
            self.gncfg = json.load(config_file)
                   
    def    gngraph_fileconvert_to_utf8(self, spath, destpath, sencode, dencode):
        #destpath = path+".utf8"
        #dest1 = "utf8-{0}".format(path)
        
        try:
             with open(spath, 'r', encoding=sencode) as inp,\
                  open(destpath, 'w', encoding=dencode) as outp:
                for line in inp:
                    outp.write(line)
        
           ### with open(spath, "rb") as source:
             ##   with open(destpath, "wb") as dest:
            ###     dest.write(source.read().decode(sencode).encode(dencode))
        except FileNotFoundError:
            print(" That file doesn't seem to exist.") 
    
    
    
    
    def    gngraph_fileencode_get(self, file_path):
        
         with open(file_path, 'rb') as rawdata:
           sres = chardet.detect(rawdata.read(100000))
         return sres["encoding"]
        
    
    def    gngraph_nodeid_init(self):
    
        nodeid_cfg_file = self.gncfg["gngraph_cfg_nodeid_filename"]
        nodeid_cfg_fpath = self.gncfg["gngraph_cfg_dir"]+"/"+nodeid_cfg_file
    
        if not path.exists(nodeid_cfg_fpath):
            print('gngraph_node_init: Initing nodeid file ')
            # Create new nodeid pkl file
            start_nodeid=0x10f0
            self.gngraph_nodeid_save(start_nodeid)         

    def   gngraph_nodeid_save(self, node_id_val):    
        nodeid_cfg_file = self.gncfg["gngraph_cfg_nodeid_filename"]
        nodeid_cfg_fpath = self.gncfg["gngraph_cfg_dir"]+"/"+nodeid_cfg_file
    
        with open(nodeid_cfg_fpath, 'wb') as fs:
           pickle.dump(node_id_val, fs)
 
    
    def    gngraph_nodeid_get(self):
       nodeid_cfg_file = self.gncfg["gngraph_cfg_nodeid_filename"]
       nodeid_cfg_fpath = self.gncfg["gngraph_cfg_dir"]+"/"+nodeid_cfg_file 
       with open(nodeid_cfg_fpath, 'rb') as fs:
           return pickle.load(fs)

       
    def    gngraph_edgeid_init(self):
        edgeid_cfg_file = self.gncfg["gngraph_cfg_edgeid_filename"]
        edgeid_cfg_fpath = self.gncfg["gngraph_cfg_dir"]+"/"+edgeid_cfg_file

        if not path.exists(edgeid_cfg_fpath):
            print('gngraph_edge_init: Initing edgeid file ')
            # Create new nodeid pkl file                                       
            start_edgeid=0x20f0
            self.gngraph_edgeid_save(start_edgeid)

    def   gngraph_edgeid_save(self, edge_id_val):
        edgeid_cfg_file = self.gncfg["gngraph_cfg_edgeid_filename"]
        edgeid_cfg_fpath = self.gncfg["gngraph_cfg_dir"]+"/"+edgeid_cfg_file

        with open(edgeid_cfg_fpath, 'wb') as fs:
           pickle.dump(edge_id_val, fs)
           
    def    gngraph_edgeid_get(self):
       edgeid_cfg_file = self.gncfg["gngraph_cfg_edgeid_filename"]
       edgeid_cfg_fpath = self.gncfg["gngraph_cfg_dir"]+"/"+edgeid_cfg_file
       with open(edgeid_cfg_fpath, 'rb') as fs:
           return pickle.load(fs)

        
    def   upload_csv_files(self):
        # Get current directory path
        current_path = pathlib.Path.cwd()
        # Get the list of csv files path present in the current dir
        csv_list=list(current_path.glob("*.csv"))
        # create list containing just the csv file names
        csv_file_list = [csv_list[index].name for index in range(len(csv_list))]

        out_folder_path_list=[]
        out_file_list=[]
        # Create the respective directories in the ouput folder 
        for count, value in enumerate(csv_list):
            file_name=csv_list[count].stem
            out_file_list.append(f"{file_name}.json")
            new_dir = current_path/"output"/file_name
            out_folder_path_list.append(str(new_dir))
            # The option parents=True creates the parent directories too if absent, no action if exists
            new_dir.mkdir(parents=True,exist_ok=True)


 

    #Read csv files and create dateframe object
    def gngraph_datafile_read(self, spark, file_path, delimchar):
        #df=spark.read.csv(file_path, header=True, inferSchema=True)
        
        with open(file_path, 'rb') as rawdata:
           sres = chardet.detect(rawdata.read(100000))
  
        print("gngraph: Input data file is encoded "+sres['encoding'])
        ###.option("encoding", sres["encoding"]) \
        ###.option("encoding", "utf-8") \
        df = spark.read \
                .option("delimiter", delimchar) \
                .option("encoding", sres["encoding"]) \
                .csv(file_path, header=True, inferSchema=True)
        return df


    # return the header 
    def get_columns(self, df):
         return df.columns

    # return column count
    def get_column_count(self,df):
        return len(df.columns)

    def get_file_name(self, path_val):
        return path_val[:-4]

        #build metanode
        # def build_meta_node(df, filename,rec_count):
        #     node_data=[{"gnnodeid":rec_count,"gnnodename":filename,"gnnodetype":"GNMetaNode"}]
        #     indx=rec_count + 1
        #     meta_nodes = get_columns(df)
        #     for count,value in enumerate(meta_nodes):
        #         node_data.append({"gnnodeid": count+indx,"gnnodename":value,"gnnodetype":"GNMetaNodeAttr"})
        #     return node_data

        #build metanode & metaedges
    def gngraph_build_meta_node(self, df, filename, gnnode_id, gnedge_id):
        node_data=[{"gnnodeid": gnnode_id, "gnnodename":filename,"gnnodetype":"GNMetaNode"}]
        edge_list=[]
        indx= gnnode_id + 1
        gindx = gnedge_id
        meta_nodes = self.get_columns(df)
        for count,value in enumerate(meta_nodes):
            node_data.append({"gnnodeid": count+indx,"gnnodename":value,"gnnodetype":"GNMetaNodeAttr"})
            edge_list.append({"gnedgeid": count+gindx, "gnedgename":"HAS_ATTR", "gnedgetype": "GNMetaNodeEdge", "gnsourceid": gnnode_id, "gntargetid": count+indx, "count": 1, "directed": "true"})
        return (node_data,edge_list)


    # write/update the meta node data to gnmeta[filename]node.json
    def   gngraph_metanode_write(self, node_data, node_name):

        ##meta_file_path=f"{dest_path}/gnmeta{wtype}.json"
        meta_file_path = self.meta_node_filepath
        print('gngraph: Writing gnmetanode json'+meta_file_path)
                
        if not path.exists(meta_file_path):    
            with open(meta_file_path,"w") as out_file:
                json.dump(node_data,out_file)
            print('gngraph: new metafile '+meta_file_path+' is created ')
            sencode = self.gngraph_fileencode_get(meta_file_path)
            print('gngraph: new metafile encoding :'+sencode);
            return    
        else:
            ### update meta file
            jsonFile = open(meta_file_path, "r") 
            cdata = json.load(jsonFile)
            jsonFile.close() # Close the JSON file

            print('gngraph: node data ')
            print(node_data)
            node_json_list = []
            for item in node_data:
                if (type(item) == str):
                    ijson = json.loads(item)
                    node_json_list.append(ijson)
                if (type(item) == dict):
                    node_json_list.append(item)

            ### Extend with new nodedata                                                                
            cdata.extend(node_json_list)

            with open(meta_file_path, "w") as out_file:
                json.dump(cdata, out_file)
     
    
 
    # write/update the meta node data to gnmeta[filename]node.json
    def   gngraph_metaedge_write(self, edge_list):

        #edge_file_path=f"{dest_path}/gnmetaedge.json"
        #print(meta_file_path)
        edge_file_path = self.meta_edge_filepath
                
        if not path.exists(edge_file_path):    
            with open(edge_file_path,"w") as out_file:
                json.dump(edge_list, out_file)
            return    
        else:
            sencode = self.gngraph_fileencode_get(edge_file_path)
            print('gngraph edge file'+edge_file_path+' encoding '+sencode)
            
            ##if (sencode != "utf-8" || sencode != "ascii" ):
             ##   print('gngrap edge file converting from '+sencode+' to utf-8')
             ##   tmp_edge_fpath = edge_file_path+".utf8" 
             ##   self.gngraph_fileconvert_to_utf8(edge_file_path, tmp_edge_fpath, "cp1252", "utf-8") 
             ##   os.rename(edge_file_path, edge_file_path+".enc")
             ##   os.rename(tmp_edge_fpath, edge_file_path)
            
            print('gngraph: json opening '+edge_file_path)
            
            ### update edge file
            jsonFile = open(edge_file_path, "r") 
            cdata = json.load(jsonFile)
            jsonFile.close() # Close the JSON file
            #print(cdata[0])
            #print(data[0])
            ### update meta information
            #cres = np.concatenate(cdata, data)
            #edge_list_json = json.loads(edge_list)
            ### toJSON return array of strings and we need to convert each one as JSON object
            print(edge_list)
            edge_json_list = []
            for item in edge_list:
                if (type(item) == str):                  
                    ijson = json.loads(item)
                    edge_json_list.append(ijson)

                if (type(item) == dict):
                    edge_json_list.append(item)
                    
            ### Extend with new edge data    
            cdata.extend(edge_json_list)

            with open(edge_file_path, "w") as out_file:
                json.dump(cdata, out_file)
            

            
    # # invoke this function with dataframe object and header col count
    # def add_gnnodeId(df, count):
    #     new_schema = StructType(df.schema.fields[:] + [StructField("gnnodeid", LongType(), False)])
    #     zipped_rdd = df.rdd.zipWithIndex()
    #     dfWithId= (zipped_rdd.map(lambda x: Row(*list(x[0]) + [x[1]+count] )).toDF(new_schema))
    #     return dfWithId

    # invoke this function with dataframe object and header col count
    def add_gnId_transform(self, df, count,type_val):
        new_schema = StructType(df.schema.fields[:] + [StructField(type_val, LongType(), False)])
        zipped_rdd = df.rdd.zipWithIndex()
        dfWithId= (zipped_rdd.map(lambda x: Row(*list(x[0]) + [x[1]+count] )).toDF(new_schema))
        return dfWithId


    # Add timestamp and metanode cols
    def add_gnnode_attr(self, df, file_name):
        dfWithAttr=df.withColumn("gntimestamp", lit(F.unix_timestamp())) \
         .withColumn("gnmetanode", lit(file_name)) 
        return dfWithAttr


    # Add gnnodename col
    def add_gnnode_name(self, df, file_name,col_name):
        df_nodename=df.withColumn("gnnodename", F.concat(F.lit(file_name), F.col(col_name)))
        return df_nodename


    # Write Datanode into json file after the transformations
    def write_gntransformed_data(self, df, filepath):
         df.write.mode("overwrite").json(filepath)

    
 
    # Add gnedgename and gnedgetype cols
    def add_gnedge_ename_etype_data(self, df,ename, etype):

        _dfWithAttr=df.withColumn("gnedgename", lit(ename)) \
                      .withColumn("gnedgetype", lit(etype)) 
        return _dfWithAttr


    # Add gnsourceid and gntargetid cols
    def add_gnedge_srctgt_datanode(self, df, sourceid):

        _dfWithAttr = df.withColumn('gnsourceid', lit(sourceid)) \
                       .withColumn('gntargetid', df['gnnodeid'])
        return _dfWithAttr



    def   gngraph_load_csv_data(self, entity_filepath):

        df_entity = read_data_file(entity_filepath)
        # get the name of the file without the extension                                              
        entity_name= get_file_name(entity_filepath)

        return (df_entity, entity_name)


    ##### Deal with one file at  a time
    def    gngraph_create_datanodes(self, df_entity, entity_name, entity_nodeid, dest_data_filepath):
        # Driver code to construct  file datanode pipeline and write the transformed file
        # ***********************************************************************************
        # Get the product dataframe object passing product.csv
        ##df_entity = read_data_file(entity_filepath)

        # get the name of the file without the extension
        ## entity_filename= get_file_name(entity_filepath)

        # Get columns presents in the file and add one for nodename
        ##col_count= get_column_count(df_entity)
        ##new_col_count = self.gngraph_nodeid_get()+col_count
        ##new_prod_col_count = new_col_count + 1
        gnnode_id = self.gngraph_nodeid_get()
        gnedge_id = self.gngraph_edgeid_get()
        print('gngraph: create data nodes node_id:'+str(gnnode_id))
        
        # Construct entity data node containing gnnodeid for each row
        df_entity_nodeid = self.add_gnId_transform(df_entity, gnnode_id, "gnnodeid")

        # # invoke further transformation to add timestamp and metanode cols
        df_entity_attr = self.add_gnnode_attr(df_entity_nodeid, entity_name)

        # # transformation to add gnnodename
        df_entity_datanode = self.add_gnnode_name(df_entity_attr,  entity_name, "gnnodeid")

        new_gnnodeid_max = df_entity_datanode.agg({"gnnodeid": "max"}).collect()[0][0]
        print('gngraph: created datanodes  new nodeidmax '+str(new_gnnodeid_max))
        
        ## Final product dataframe with all the transformations added
        #product_list=["productid","prodname","gnnodeid", "gnnodename","gnmetanode","gntimestamp"]
        #gnnode_prod_datanodes_df = df_prod_datanode.select(product_list)
        # # invoke an action to show the first 5 rows of the selected column
        #gnnode_prod_datanodes_df.show(5)

        # Edge transformations
        df_entity_edge_edgeid = self.add_gnId_transform(df_entity_datanode, gnedge_id, "gnedgeid")
        df_entity_edge_2 = self.add_gnedge_ename_etype_data(df_entity_edge_edgeid, "IS", "GNDataNodeEdge")
        #df_entity_edge_2.select(["gnedgeid","gnedgename","gnedgetype"]).show(5) 
        df_entity_edge_3 = self.add_gnedge_srctgt_datanode(df_entity_edge_2, entity_nodeid)
        df_entity_edge = df_entity_edge_3.select(["gnedgeid", "gnedgename", "gnedgetype", "gnsourceid", "gntargetid"])
        
           #### Conver edge data to utf-8 
        ######df_entity_edge = df_entity_edge_unicode.rdd.map(lambda x: x.encode("ascii","ignore"))
        
        new_gnedgeid_max = df_entity_edge.agg({"gnedgeid": "max"}).collect()[0][0]
        
        ###new_gnedgeid_max = df_entity_edge.select(max("gnedgeid")).collect()[0][0]
                
        print('gngraph: created edgenodes  new edgeidmax '+str(new_gnedgeid_max))
        
        ## Write the transformed data as json file in customer folder
        self.write_gntransformed_data(df_entity_datanode, dest_data_filepath)
       
        
        ### Convert data_entity_edge to list of JSON and then append to edge file(gnedges.json)
        ###edge_list = df_entity_edge.option("encoding", "UTF-8").toJSON().collect()
        edge_list = df_entity_edge.toJSON().collect()
        print(edge_list)
        ####write_gntransformed_data(df_entity_edge, dest_edge_filepath)
        ###self.write_gntransformed_edgedata(edge_list, dest_edge_filepath)

        self.gngraph_metaedge_write(edge_list)
        
        self.gngraph_nodeid_save(new_gnnodeid_max)
        self.gngraph_edgeid_save(new_gnedgeid_max)


    def    gngraph_metanodes_create(self, df_entity, entity_name):

        meta_node_path = self.gncfg["gngraph_data_dir"]
        print("gngraph create metanodes at path: "+meta_node_path)
        gnnode_id_max = self.gngraph_nodeid_get()
        gnedge_id_max = self.gngraph_edgeid_get()
        print('gngraph create metanodes nodeidmax '+str(gnnode_id_max))
        print('gngraph create metanodes edgeidmax '+str(gnedge_id_max))
        entity_nodeid = gnnode_id_max
        entity_edgeid = gnedge_id_max
        
        node_data,edge_data= self.gngraph_build_meta_node(df_entity, entity_name, gnnode_id_max, gnedge_id_max)
        
        new_gnnodeid_max = gnnode_id_max + len(node_data)
        new_gnedgeid_max = gnedge_id_max + len(edge_data)
        
        print('gngraph: saving new gnnode idmax '+str(new_gnnodeid_max))
        print('gngraph: saving new gnedge idmax '+str(new_gnedgeid_max))
        self.gngraph_nodeid_save(new_gnnodeid_max)
        self.gngraph_edgeid_save(new_gnedgeid_max)
        
        self.gngraph_metanode_write(node_data, entity_name)
        ###self.gngraph_metanode_write(edge_data, meta_node_path, entity_name, "edge")
        self.gngraph_metaedge_write(edge_data)
        
        
        return (entity_nodeid, entity_edgeid)


def     gngraph_ingest_file_api(filename, delimchar, gngraph_cls):
   
    ###gngraph_config_init(gngraph_cfg_data, rtDir)
    
    #inpfile="Customer.csv"
    inpfile = filename
    inpfilepath = gngraph_cls.gncfg["gngraph_uploads_dir"]+"/"+inpfile
    entity_name = inpfile.rsplit(".", 1)[0]
    
    ### Make sure source file is in utf-8 format or change it to utf-8
    sencode = gngraph_cls.gngraph_fileencode_get(inpfilepath)
    print("gngraph: source file "+inpfilepath+" encoding "+sencode)
    
    if (sencode != "utf-8"):
        ## change new inpfile     
        sfilename = entity_name+".utf8.csv";
        sfilepath = gngraph_cls.gncfg["gngraph_uploads_dir"]+"/"+sfilename
        gngraph_cls.gngraph_fileconvert_to_utf8(inpfilepath, sfilepath, sencode, "utf-8") 
        ### Also convert any cp1252 characters as well
        #gngraph_cls.gngraph_fileconvert_to_utf8(inpfilepath, sfilepath, "cp1252", "utf-8") 
    else:
        sfilename = inpfile
        sfilepath = inpfilepath
        
    print("gngraph: Ingest source file "+sfilepath)
    print("gngraph: Ingest entity "+entity_name)
    

    df = gngraph_cls.gngraph_datafile_read(gngraph_cls.spark, sfilepath, delimchar)
    df.printSchema()
    entity_nodeid, entity_edgeid = gngraph_cls.gngraph_metanodes_create(df, entity_name)

    ##data folder
    entity_data_folder = gngraph_cls.gncfg["gngraph_data_dir"]+"/"+entity_name
    if not os.path.isdir(entity_data_folder):
       os.mkdir(entity_data_folder)    
    
    entity_data_file = entity_name+".json";
    dest_entity_data_filepath = entity_data_folder+"/"+entity_data_file
    
    gngraph_cls.gngraph_create_datanodes(df, entity_name, entity_nodeid, dest_entity_data_filepath)
    

def      gngraph_init(rootDir):

    app_name = "gngraph"
    gngraph_cls = gnGraphIngest(rootDir)

    print(' Gngraph Init no spark Context')
    ### Set spark session
    ##spark = SparkSession.builder.appName(app_name).getOrCreate()

    return gngraph_cls

if __name__ == "__main__":

    print("Starting gn ingest file")
    curDir = os.getcwd()
    rtDir = curDir.rsplit('/', 1)[0]
    app_name="gngraph"

    if rtDir not in sys.path:
        sys.path.append(rtDir)

    ###gngraph_cls = gngraph_ingest(rtDir)
    gngraph_cls = gnGraphIngest(rtDir)

    ### Set spark session
    gngraph_cls.spark = SparkSession.builder.appName(app_name).getOrCreate()

    fname = "Product.csv"
    gngraph_ingest_file_api(fname, "|", gngraph_cls)
    print(" gngraph ingest completed for "+fname)

    ##fname = "Customer.csv"
    ##gngraph_ingest_file_api(fname, "|", gngraph_cls)
    ##print(" gngraph ingest completed for "+fname)

