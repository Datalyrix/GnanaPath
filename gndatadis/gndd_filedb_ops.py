
from tinydb import TinyDB, Query, where
import os
import re
from datetime import datetime

class FileLogSchemaModel:

    query = Query()

    def __init__(self, db_path):
        dbpath = os.path.join(db_path, 'filelogdb.json')
        self._db = TinyDB(dbpath)

    def req_fields_json(self, dict_result):
        req_items = ['flid', 'filename', 'filesize', 'filetype', 'uploadtm', 'ingesttm', 'filedesc', 'filedelim', 'fileencode', 'tags', 'state']
        return {key: value for key, value in dict_result.items()
                if key in req_items}

    def search_op(self, req_dict):
        ####exists = self.search_res(req_dict['db']) && self.search_res(req_dict['schema']) && self.search_res(req_dict['table']) && self.search_res(req_dict['fieldname'])
        exists = self._db.search((FileLogSchemaModel.query.filename == req_dict['filename']) & (FileLogSchemaModel.query.filetype == req_dict['filetype']))

        return True if exists else False

    def get_allrecs(self):
        return self._db.search(FileLogSchemaModel.query.flid > 0)
            
    def search_res(self, filename):
        return self._db.search(FileLogSchemaModel.query.filename == filename)

    
    def search_file(self, filename):
        ctentry = Query();
        #res_info = self._db.search(ctentry.fieldname == fieldname)
        res_info = self._db.search(ctentry.filename.search(filename,flags=re.IGNORECASE))
        return res_info

    def search_file_byid(self, fileid):
        ctentry = Query();
        res_info = self._db.search(ctentry.flid.search(fileid))
        return res_info
    
    def get_total_recs(self):
        ctqry = Query();
        dlen = self._db.count(ctqry.flid > 0)
        return dlen
            
    def insert_op(self, req_dict):
        if not self.search_op(req_dict):
            rec_id = self._db.__len__()
            req_dict['flid'] = rec_id+1;
            self._db.insert(req_dict)
            return self._db.all()
        return "None_Insert"

    def delete_op(self, req_dict):
        if self.search_op(req_dict):
            self._db.remove(where('fieldname') == req_dict['fieldname'])
            return self._db.all()
        return "None_Delete"

    def update_op(self, old_fieldname, req_dict):
        if not self.search_res(old_fieldname):
            return False
        self._db.update({'db': req_dict['db'],
                         'schema': req_dict['schema'],
                         'table': req_dict['table'],
                         'fieldname': req_dict['fieldname'],
                         'fielddtype': req_dict['fielddtype']
                         },
                        SchemaModel.query.fieldname == old_fieldname)

        return self._db.all()
     

    
    def stop_db(self):
        self._db.close()



def   gndd_filedb_insert_file_api(fname, fsize, ftype, fdesc, fdelim, flogdbpath):

     # datetime object containing current date and time
     now = datetime.now()
     # dd/mm/YY H:M:S
     dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    
     req_d = {}
     flogDBConnp = FileLogSchemaModel(flogdbpath)
     flidmax = flogDBConnp.get_total_recs()+1
     req_d["flid"] = flidmax
     req_d["filename"] = fname
     req_d["filesize"] = fsize
     req_d["filetype"] = ftype
     req_d["uploadtm"] = dt_string
     req_d["filedesc"] = fdesc
     req_d["filedelim"] = fdelim
     req_d["fileencode"] = "utf-8"
     req_d["tags"] = []
     req_d["state"] = "UPLOADED";

     status = flogDBConnp.insert_op(req_d)

     if (status == "None_Insert"):
         return -1
     else:
         return 0
     
     
def  gndd_filedb_filelist_get(flogdbpath):

    fres = {};
    flogDBConnp = FileLogSchemaModel(flogdbpath)
    fres = flogDBConnp.get_allrecs()
    return fres
