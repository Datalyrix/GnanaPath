import os
import logging
from tinydb import TinyDB, Query, where

"""
 gn_config Module : sets the directory and sets the config dir config vars

GNGrapConfigModel: file creds/gngraph/gngraph_config_settings.json
GNGraphDBConfigModel: file creds/gngraph/gngraph_pgres_dbcreds.json

"""


class GNGraphDBConfigModel:

    query = Query()

    def __init__(self, db_path):
        
        dbpath = os.path.join(db_path, 'gngraph_pgres_dbcreds.json')
        if (os.path.exists(dbpath)): 
           self._db = TinyDB(dbpath)
        else:
           def_dict = {'serverIP': '', 'serverPort': '', 'username': '', 'password': '', 'dbname':''}
           self._db = TinyDB(dbpath)
           self.insert_op(def_dict)
        
    def req_fields_json(self, dict_result):
        req_items = ['serverIP', 'serverPort', 'username', 'password', 'dbname']
        return {key: value for key, value in dict_result.items()
                if key in req_items}

    def search_op(self, req_dict):
        return True if self.search_res(1) else False

    def search_res(self, id):
        return self._db.get(doc_id=id)

    
    def insert_op(self, req_dict):
        if not self.search_op(req_dict):
            self._db.insert(req_dict)
            return self._db.all()
        return "None_Insert"
    
    def upsert_op(self, req_dict):
        self._db.upsert(req_dict, GNGraphDBConfigModel.query.serverIP == req_dict['serverIP'])    

    def delete_op(self, req_dict):
        if self.search_op(req_dict):
            self._db.remove(where('serverIP') == req_dict['serverIP'])
            return self._db.all()
        return "None_Delete"

    def get_op(self):
        return self._db.get(doc_id=1)
    
    def update_op(self, old_srv_IP, req_dict):
        if not self.search_res(old_srv_IP):
            return False
        self._db.update({'serverIP': req_dict['serverIP'],
                         'serverPort': req_dict['serverPort'],
                         'username': req_dict['username'],
                         'password': req_dict['password']},
                        GNGraphDBConfigModel.query.serverIP == old_srv_IP)

        return self._db.all()

    def stop_db(self):
        self._db.close()


class GNGraphConfigModel:

    query = Query()

    def __init__(self, db_path):
        dbpath = os.path.join(db_path, 'gngraph_config_settings.json')
        if (os.path.exists(dbpath)): 
           self._db = TinyDB(dbpath)
        else:
           def_dict = {'sfmode': 1, 'dbmode': 0}
           self._db = TinyDB(dbpath)
           self.insert_op(def_dict)
           
    def req_fields_json(self, dict_result):
        req_items = ['sfmode', 'dbmode']
        return {key: value for key, value in dict_result.items()
                if key in req_items}

    def search_op(self, req_dict):
        return True if self.search_res(1) else False

    def search_res(self, id):
        return self._db.get(doc_id=id)

    def update_op(self, req_dict):
         
         self._db.update({'sfmode': req_dict['sfmode'] , 'dbmode': req_dict['dbmode']}, doc_id == 1)
    
    def insert_op(self, req_dict):
        if not self.search_op(1):
            self._db.insert(req_dict)
            return self._db.all()
        else:
            self.update_op(req_dict)
        return "None_Insert"

    def delete_op(self, req_dict):
        if self.search_op(req_dict):
            self._db.remove(where('serverIP') == req_dict['serverIP'])
            return self._db.all()
        return "None_Delete"

    def get_op(self):
        return self._db.get(doc_id=1)
    
    def update_op(self, req_dict):
        if not self.search_res(1):
            return False
        self._db.update({'sfmode': req_dict['sfmode'], 'dbmode': req_dict['dbmode']},  GNGraphConfigModel.query.id == 1)

        return self._db.all()

    def stop_db(self):
        self._db.close()

"""
gn_config_init: Main init config routine 
"""

def        gn_config_init(app):
    
    app.config["gnDataFolder"] = app.config["gnRootDir"]+"/gndata"
    app.config["gnDBFolder"] = app.config["gnRootDir"]+"/gndb"
    app.config["gnCfgDBFolder"] = app.config["gnRootDir"]+"/gnconfigdb"
    app.config["gnUploadsFolder"] = app.config["gnDataFolder"]+"/uploads"
    app.config["gnDiscoveryFolder"] = app.config["gnDataFolder"] +"/datadiscovery"
    app.config["gnProfileFolder"] = app.config["gnDiscoveryFolder"]+"/profile"
    app.config["gnGraphFolder"] = app.config["gnDataFolder"]+ "/gngraph"
    app.config["gnGraphDBCredsFolder"] = app.config["gnRootDir"]+"/creds/gngraph"  
    
    if not os.path.isdir(app.config["gnDataFolder"]):
        os.mkdir(app.config["gnDataFolder"])
        print(app.config["gnDataFolder"]+" is created")
    if not os.path.isdir(app.config["gnUploadsFolder"]):
        os.mkdir(app.config["gnUploadsFolder"])
        print(app.config["gnUploadsFolder"]+" is created")
    if not os.path.isdir(app.config["gnCfgDBFolder"]):
        os.mkdir(app.config["gnCfgDBFolder"])
        print(app.config["gnCfgDBFolder"]+" is created")    
    if not os.path.isdir(app.config["gnDiscoveryFolder"]):
        os.mkdir(app.config["gnDiscoveryFolder"])
        print(app.config["gnDiscoveryFolder"]+" is created ")
    if not os.path.isdir(app.config["gnProfileFolder"]):
        os.mkdir(app.config["gnProfileFolder"])
        print(app.config["gnProfileFolder"]+" is created")
    if not os.path.isdir(app.config["gnGraphFolder"]):
        os.mkdir(app.config["gnGraphFolder"])
        print(app.config["gnGraphFolder"]+" is created")
   
    app.config["gnLogDir"] = app.config["gnRootDir"]+"/gnlog"
    app.config["gnLogFile"] = "gnpath.log"

    ###Read Config settings
    gncfg = GNGraphConfigModel(app.config["gnGraphDBCredsFolder"])
    
    app.config["gncfg_settings"] = gncfg.get_op()

    gndb_cfg = GNGraphDBConfigModel(app.config['gnGraphDBCredsFolder'])
    app.config["gndbcfg_settings"] = gndb_cfg.get_op()
                                    
    
    
        
def     gn_logging_init(logname):
    
    path = os.getcwd()
    gnRootDir = path.rsplit('/', 1)[0]
    gnLogDir = gnRootDir+"/gnlog"

    if not os.path.isdir(gnLogDir):
        os.mkdir(gnLogDir)

    gnLogFile = "gnpath.log"
    logfilepath = gnLogDir+"/"+gnLogFile     
    logging.basicConfig(filename=logfilepath, filemode='a', \
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',\
                        level=logging.INFO)
    logger = logging.getLogger(logname)
    return logger


gnlogger = gn_logging_init("GNPath")


def    gn_log(msg):
    gnlogger.info(msg)


def    gn_log_err(msg):
    gnlogger.error(msg)



