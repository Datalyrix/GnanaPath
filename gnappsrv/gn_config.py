import os
import sys
from tinydb import TinyDB, Query, where
from tinydb.table import Document
###from time import time, ctime
from datetime import datetime

"""
 gn_config Module : sets the directory and sets the config dir config vars

GNGrapConfigModel: file creds/gngraph/gngraph_config_settings.json
GNGraphDBConfigModel: file creds/gngraph/gngraph_pgres_dbcreds.json

"""
curentDir = os.getcwd()
listDir = curentDir.rsplit('/', 1)[0]
sys.path.append(listDir)

from gnutils.gn_log import gn_log, gn_log_err

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
        ##self._db.upsert(req_dict, GNGraphDBConfigModel.query.serverIP == req_dict['serverIP'])
        self._db.upsert(Document(req_dict, doc_id=1))

    def delete_op(self, req_dict):
        if self.search_op(req_dict):
            self._db.remove(where('serverIP') == req_dict['serverIP'])
            return self._db.all()
        return "None_Delete"

    def get_op(self):
        ###return self._db.get(GNGraphDBConfigModel.query.id == 1)
        return self._db.get(doc_id=1)
    
    def update_rec(self, req_dict):        
      self._db.update({'serverIP': req_dict['serverIP'] , 'serverPort': req_dict['serverPort'], 'username': req_dict['username'], 'password': req_dict['password'], 'dbname': req_dict['dbname']}, GNGraphDBConfigModel.query.id == 1)

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

    def upsert_op(self, req_dict):
        ##self._db.upsert(req_dict, GNGraphDBConfigModel.query.serverIP == req_dict['serverIP'])
        self._db.upsert(Document(req_dict, doc_id=1))

    
    
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


class GNGraphDatabaseListModel:

    query = Query()

    def __init__(self, db_path):
        dbpath = os.path.join(db_path, 'gngraph_dblist.json')
        ##if (os.path.exists(dbpath)): 
        self._db = TinyDB(dbpath)
        #else:
        #   def_dict = {'dbname': 'gngraph', 'dbmode': 0}
        #   self._db = TinyDB(dbpath)
        #   self.insert_op(def_dict)
           
    def req_fields_json(self, dict_result):
        req_items = ['id', 'dbname', 'dbtype', 'createdOn']
        return {key: value for key, value in dict_result.items()
                if key in req_items}

    def search_db_exists(self, req_dict):
        exists = self._db.search((GNGraphDatabaseListModel.query.dbname == req_dict['dbname'])   & (GNGraphDatabaseListModel.query.dbtype == req_dict['dbtype']))
        return True if exists else False

    def search_db(self, req_dict):        
        return  self._db.search((GNGraphDatabaseListModel.query.dbname == req_dict['dbname'])   & (GNGraphDatabaseListModel.query.dbtype == req_dict['dbtype']))

    def search_db_byid(self, id):
        return  self._db.search(GNGraphDatabaseListModel.query.id == id)
    
    def search_get_all_dbs(self):
        return self._db.all()    

    def insert_db_op(self, req_dict):
        if not self.search_db(req_dict):
            rec_id = self._db.__len__()
            req_dict['id'] = rec_id+1;
            ###req_dict['tags'] = '';
            self._db.insert(req_dict)
            ##return self._db.all()
            return req_dict['id']
        return "None_Insert"

    def delete_op(self, req_dict):
        srec =  self.search_db(req_dict)
        if srec:
            self._db.remove(where('id') == req_dict['id'])
            return self._db.all()
        return "None_Delete"

    def update_state_op(self, req_dict):
        if not self.search_db_byid(req_dict['id']):
            return False
        self._db.update({'state': req_dict['state']
                         },
                        GNGraphDatabaseListModel.query.id == req_dict['id'])        
        return True
     

    def search_res(self, id):
        return self._db.get(doc_id=id)

    def insert_op(self, req_dict):
        if not self.search_op(1):
            self._db.insert(req_dict)
            return self._db.all()
        else:
            self.update_op(req_dict)
            return "None_Insert"

    def upsert_op(self, req_dict):
        ##self._db.upsert(req_dict, GNGraphDBConfigModel.query.serverIP == req_dict['serverIP'])
        self._db.upsert(Document(req_dict, doc_id=1))

    
    def get_op(self):
        return self._db.get(doc_id=1)
    


class GNGraphBizDomainListModel:

    query = Query()

    def __init__(self, db_path):
        dbpath = os.path.join(db_path, 'gngraph_bizdomains.json')
        ##if (os.path.exists(dbpath)): 
        self._db = TinyDB(dbpath)
        #else:
        #   def_dict = {'dbname': 'gngraph', 'dbmode': 0}
        #   self._db = TinyDB(dbpath)
        #   self.insert_op(def_dict)
           
    def req_fields_json(self, dict_result):
        req_items = ['id', 'bizdomain', 'type', 'db', 'createdOn']
        return {key: value for key, value in dict_result.items()
                if key in req_items}

    def search_db_exists(self, req_dict):
        exists = self._db.search((GNGraphBizDomainsListModel.query.bizdomain == req_dict['bizdomain'])   & (GNGraphBizDomainsListModel.query.db == req_dict['db']))
        return True if exists else False

    def search_bizdomain(self, req_dict):        
        return  self._db.search((GNGraphBizDomainListModel.query.bizdomain == req_dict['bizdomain'])   & (GNGraphBizDomainListModel.query.db == req_dict['db']))

    def search_bizdomain_byid(self, id):
        return  self._db.search(GNGraphBizDomainListModel.query.id == id)

    def search_bizdomain_bydb(self, db):
        return  self._db.search(GNGraphBizDomainListModel.query.db == db)

    def search_get_all_bizdomains(self):
        return self._db.all()    

    def insert_bizdomain_op(self, req_dict):
        if not self.search_bizdomain(req_dict):
            rec_id = self._db.__len__()
            req_dict['id'] = rec_id+1;
            ###req_dict['tags'] = '';
            self._db.insert(req_dict)
            ##return self._db.all()
            return req_dict['id']
        return "None_Insert"

    def delete_op(self, req_dict):
        srec =  self.search_db(req_dict)
        if srec:
            self._db.remove(where('id') == req_dict['id'])
            return self._db.all()
        return "None_Delete"

    def update_state_op(self, req_dict):
        if not self.search_bizdomain_byid(req_dict['id']):
            return False
        self._db.update({'state': req_dict['state']
                         },
                        GNGraphBizDomainsListModel.query.id == req_dict['id'])        
        return True
     

    def search_res(self, id):
        return self._db.get(doc_id=id)

    def insert_op(self, req_dict):
        if not self.search_op(1):
            self._db.insert(req_dict)
            return self._db.all()
        else:
            self.update_op(req_dict)
            return "None_Insert"

    def upsert_op(self, req_dict):
        ##self._db.upsert(req_dict, GNGraphDBConfigModel.query.serverIP == req_dict['serverIP'])
        self._db.upsert(Document(req_dict, doc_id=1))

    
    def get_op(self):
        return self._db.get(doc_id=1)
    
    
"""
gn_config_init: Main init config routine 
"""

def        gn_config_init(app):
    
    gn_log('GnCfg: Initializing Config directories ')
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
        gn_log('GnCfg: '+app.config["gnDataFolder"]+" is created")
    if not os.path.isdir(app.config["gnUploadsFolder"]):
        os.mkdir(app.config["gnUploadsFolder"])
        gn_log('GnCfg: '+app.config["gnUploadsFolder"]+" is created")

    if not os.path.isdir(app.config["gnCfgDBFolder"]):
        os.mkdir(app.config["gnCfgDBFolder"])
        gn_log('GnCfg: '+app.config["gnCfgDBFolder"]+" is created")    
    if not os.path.isdir(app.config["gnDiscoveryFolder"]):
        os.mkdir(app.config["gnDiscoveryFolder"])
        gn_log('GnCfg: '+app.config["gnDiscoveryFolder"]+" is created ")
    if not os.path.isdir(app.config["gnProfileFolder"]):
        os.mkdir(app.config["gnProfileFolder"])
        gn_log('GnCfg: '+app.config["gnProfileFolder"]+" is created")
    if not os.path.isdir(app.config["gnGraphFolder"]):
        os.mkdir(app.config["gnGraphFolder"])
        
    # check if config and data directory are part of gngraph
    cfg_dir = app.config["gnGraphFolder"]+"/config"
    data_dir = app.config["gnGraphFolder"]+"/data"
    if not os.path.isdir(cfg_dir):
        os.mkdir(cfg_dir)
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)
    gn_log('GnCfg: '+app.config["gnGraphFolder"]+" and subdirs are created")    
          
    app.config["gnLogDir"] = app.config["gnRootDir"]+"/gnlog"
    app.config["gnLogFile"] = "gnpath.log"
    app.config["gnLogFilePath"] = app.config["gnLogDir"]+"/"+app.config["gnLogFile"]
    
    ###Read Config settings
    gncfg = GNGraphConfigModel(app.config["gnGraphDBCredsFolder"])
    gncfg_settings = gncfg.get_op()
    
    #app.config["gnCfgSettings"] = gncfg.get_op()
    gndb_cfg = GNGraphDBConfigModel(app.config['gnGraphDBCredsFolder'])
    gndb_cfg_settings = gndb_cfg.get_op()
    gncfg_settings["gnDBCfgSettings"] = gndb_cfg_settings    
    app.config["gnCfgSettings"] = gncfg_settings

    gndb_list_cfg = GNGraphDatabaseListModel(app.config['gnGraphDBCredsFolder'])
    
    ##gndb_list["gnDBList"] = gndb_list_settings
    ##tm = time()
    ##crtm = ctime(tm)
    tm = datetime.now()
    
    req_dict = {'dbname': 'gngraph', 'dbtype': 'static', 'created': str(tm)}
    gndb_list_cfg.insert_db_op(req_dict)

    gndb_list = gndb_list_cfg.search_get_all_dbs()
    app.config["gnDBList"] = gndb_list
    app.config["gnDBListCfg"] = gndb_list_cfg

    ###Add schemas 
    gndb_bizdom_list_cfg = GNGraphBizDomainListModel(app.config['gnGraphDBCredsFolder'])
    
    ##gndb_list["gnDBList"] = gndb_list_settings
    ##tm = time()
    ##crtm = ctime(tm)
    tm = datetime.now()
    ### Add default bizdomains: customer, product, sales
    req_dict = {'bizdomain': 'customer', 'type': 'bizdomain', 'db': 'gngraph', 'created': str(tm)}
    gndb_bizdom_list_cfg.insert_bizdomain_op(req_dict)
    
    req_dict = {'bizdomain': 'product', 'type': 'bizdomain', 'db': 'gngraph', 'created': str(tm)}
    gndb_bizdom_list_cfg.insert_bizdomain_op(req_dict)

    req_dict = {'bizdomain': 'sales', 'type': 'bizdomain', 'db': 'gngraph', 'created': str(tm)}
    gndb_bizdom_list_cfg.insert_bizdomain_op(req_dict)

    db = "gngraph"
    gndb_bizdom_list = gndb_bizdom_list_cfg.search_bizdomain_bydb(db)
    app.config["gndbBizDomList"] = gndb_bizdom_list
    app.config["gndbBizDomListCfg"] = gndb_bizdom_list_cfg

    


    
def   gn_cfg_gngraph_dblist(gndb_list_cfg):
    
    ##gndb_list_cfg = GNGraphDatabaseListModel(app.config['gnGraphDBCredsFolder'])
    
    ##gndb_list["gnDBList"] = gndb_list_settings
    gndb_list = gndb_list_cfg.search_get_all_dbs()    
    return gndb_list

def   gn_cfg_bizdomain_list_bydb(gndb_bizdom_list_cfg, db):
    
    ##gndb_list_cfg = GNGraphDatabaseListModel(app.config['gnGraphDBCredsFolder'])
    
    ##gndb_list["gnDBList"] = gndb_list_settings
    ##gndb_list = gndb_list_cfg.search_get_all_dbs()
    gndb_bizdom_list = gndb_bizdom_list_cfg.search_bizdomain_bydb(db)
    return gndb_bizdom_list
    
def   gn_pgresdb_getconfiguration(credfpath):
    
    gndb_cfg = GNGraphDBConfigModel(credfpath)
    gn_log('GnDBCfg: getting postgres config ')
    gndb_cfg_settings = gndb_cfg.get_op()
    return gndb_cfg_settings


def   gn_cfg_getaccessmode(credfpath):
    
    gncfg = GNGraphConfigModel(credfpath)
    gncfg_settings = gncfg.get_op()
    gn_log('GnCfg: getting config accessmode ')
    return gncfg_settings


