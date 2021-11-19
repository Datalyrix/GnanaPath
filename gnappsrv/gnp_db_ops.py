
from tinydb import TinyDB, Query, where
import os


class GNGraphDBConfigModel:

    query = Query()

    def __init__(self, db_path):
        dbpath = os.path.join(db_path, 'gngraph_pgres_dbcreds.json')
        self._db = TinyDB(dbpath)

    def req_fields_json(self, dict_result):
        req_items = ['serverIP', 'serverPort', 'username', 'password', 'dbname']
        return {key: value for key, value in dict_result.items()
                if key in req_items}

    def search_op(self, req_dict):
        return True if self.search_res(req_dict['serverIP']) else False

    def search_res(self):
        return self._db.search(doc_id == 1)

    
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
        self._db = TinyDB(dbpath)

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

