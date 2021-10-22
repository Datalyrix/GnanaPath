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
from moz_sql_parser import parse


import os,sys
curentDir = os.getcwd()
parentDir = curentDir.rsplit('/', 1)[0]
print(parentDir)
rootDir = parentDir.rsplit('/', 1)[0]
print(rootDir)
if parentDir not in sys.path:
    sys.path.append(parentDir)
if rootDir not in sys.path:
    sys.path.append(rootDir)
    
from gngraph.config.gngraph_config import GNGraphConfig
from gngraph.gngraph_dbops.gngraph_pgresdbops_srch import GNGraphSrchPgresDBOps
from gngraph.gngraph_dbops.gngraph_staticfileops_srch import GNGraphSrchStaticFileOps



class     GNGraphSqlParserOps:


    def    __init__(self, sql_stmt):
        self.__parser_info="moz-sql-parser"
        self.__sql_stmt = sql_stmt
        self.__sql_parsed_obj = parse(sql_stmt)
        print('GNSqlParser: ')
        print(self.__sql_parsed_obj)
        self.__entlist = None
        self.__alias_entlist = None
        self.__from_str = None
        self.gn_sqlp_from_processing(self.__sql_parsed_obj['from'])
        self.__select_ent_list = self.gn_sqlp_select_processing(self.__sql_parsed_obj.get('select'))                              
               
    def get_entlist(self):
        return self.__entlist

    
    def listwrap(self, value):
         if value is None:
               return []
         elif isinstance(value, list):
               return value
         else:
               return [value]

    
    def     chk_get_condition_str(self, cond_str):

        frm_str=''
        ceq = cond_str.get('eq')
        if  ceq is not None:
            frm_str += ""+str(ceq[0])+"="+str(ceq[1])+" "
        
        cgt = cond_str.get('gt')
        if  cgt is not None:
            frm_str += ""+str(cgt[0])+">"+str(cgt[1])+" "
                
        cgte = cond_str.get('gte')
        if  cgte is not None:
            frm_str += ""+str(cgte[0])+">="+str(cgte[1])+" "
               
        clt = cond_str.get('lt')
        if  clt is not None:
            frm_str += ""+str(clt[0])+">"+str(clt[1])+" "
                
        clte = cond_str.get('lte')
        if  clte is not None:
            frm_str += ""+str(clte[0])+">="+str(clte[1])+" "
        return frm_str            

    
    def      convert_logicalop_conditions(self, osc):
          astr = ""
          o_and = osc.get('and')
          o_or = osc.get('or')
          if (o_and is not None):
              print('  AND processing ..')
              o_and_t = self.listwrap(o_and)
              fstr1 = self.convert_logicalop_conditions(o_and_t[0])
              fstr2 = self.convert_logicalop_conditions(o_and_t[1])
              astr = fstr1+" AND "+fstr2  
           
          elif (o_or is not None):
              print('   OR Processing ..')
              o_or_t = self.listwrap(o_or) 
              fstr1 = self.convert_logicalop_conditions(o_or_t[0])
              fstr2 = self.convert_logicalop_conditions(o_or_t[1])
              astr = fstr1+" OR "+fstr2  
          else:                
              #o_c = listwrap(osc)   
              fstr = self.chk_get_condition_str(osc)
              #fstr2 = chk_get_condition_str(o_c[1])   
              #astr += fstr1+" "+cond+" "+fstr2
              astr = fstr
              print(astr)
            
          return astr

    def     gn_sqlp_select_processing(self, sel_obj):        
        selobj_list = self.listwrap(sel_obj)
        entlist=[]   
    
        for sobj in selobj_list:
            
            if isinstance(sobj, str):
               entlist.append(sobj)
               continue
               
            x=self.listwrap(sobj.get("value"))    
            if (len(x)>0):
                #c=listwrap(f.get('name'))
                entlist.append(x[0])      
            
        return entlist



    def      process_join_stmt(self, f, join_type):
       
            entlist = []
            aentlist = []
            frm_str = " "+join_type
            join_s = self.listwrap(f.get(join_type))
            
            if (len(join_s) > 0):
               for js in join_s: 
                  e = js.get('name')
                  v = js.get('value')
                  frm_str += " "+v+" as "+e
                  entlist.append(v)
                  aentlist.append(e)
            
            ## check on
            on_s = self.listwrap(f.get('on'))
            if (len(on_s) > 0):
                rstr = self.convert_logicalop_conditions(on_s[0])
                frm_str += " ON "+rstr
                print('----------------') 
                
            return (frm_str, entlist, aentlist)
    

        

    def      gn_sqlp_from_processing(self, from_obj):    
    
      frm_str="from"
      entlist=[]
      aentlist=[]
         
      ##frm_obj = self.__sql_parsed_obj['from']

      
      frm_list = self.listwrap(from_obj)
      print('GnSqlParsers: frm list ')
      print(frm_list)
      for f in frm_list:

        if isinstance(f, str):
            entlist.append(f)
            frm_str += " "+f
            continue
            
        # first check entities
        x=self.listwrap(f.get("value"))    
        if (len(x)>0):
            c=self.listwrap(f.get('name'))
            n = x[0]+" as "+c[0]
            frm_str += " "+n
            entlist.append(x[0])
            aentlist.append(c[0])
     
         

        joinprocess_flag = 0    
        i=self.listwrap(f.get("inner join"))
        j=self.listwrap(f.get("join"))
        k=self.listwrap(f.get("outer join"))
        if (len(i) > 0):
            join_type = "inner join"
            joinprocess_flag = 1
        elif (len(j) > 0):
            join_type = "join"
            joinprocess_flag = 1
        elif (len(k) > 0):
            join_type = "outer join"
            joinprocess_flag = 1
        else:
            joinprocess_flag = 0
            
        if (joinprocess_flag == 1):
            print(' Processing Join  type '+join_type)
            (fstr, elist, alist) =  self.process_join_stmt(f, join_type)
            frm_str += fstr
            entlist = entlist+elist
            aentlist = aentlist+alist
            
      self.__entlist = entlist
      self.__alias_entlist = aentlist
      self.__from_str = frm_str

      return (self.__entlist, self.__alias_entlist, self.__from_str)

    def get_entlist(self):
        return self.__entlist

    def get_entlist_alias(self):
        return self.__alias_entlist
    def get_from_str(self):
        if (self.__from_str):
            return self.__from_str
        else:
            self.gn_sqlp_from_processing(self.__sql_parsed_obj.get('from'))
            return self.__from_str
        
    def get_where_str(self):
        key="where"
        if (key in self.__sql_parsed_obj):
            where_obj = self.__sql_parsed_obj['where']
            return self.gn_sqlp_where_processing(where_obj)
        else:
            where_str = ""
            return where_str
    
    def      gn_sqlp_where_processing(self, where_obj):

        where_str="where"
        where_list = self.listwrap(where_obj)
        for f in where_list:        
            if (len(f) > 0):
                rstr = self.convert_logicalop_conditions(f)
                where_str += " "+rstr
       
        return where_str

    def  gn_sqlp_reformat_qry(self):

        selstr = ''
        selent_list = self.__select_ent_list
        ## prepare original SQL
        if (self.__alias_entlist[0] is not None):
           nodeid_sel = self.__alias_entlist[0]+"."+"gnnodeid"
        else:
           nodeid_sel = "gnnodeid"

        selent_list.append(nodeid_sel)
        selstr = 'select'
        first_flag = 0
        for s in selent_list:
          if (first_flag > 0):
              selstr += ","
          selstr += " "+s
          first_flag+=1

        self.__reformatted_sql = selstr
        return selstr
    

def        gn_srch_sqlp_api(sqlst):

    gn_sqlp_cls = GNGraphSqlParserOps(sqlst)
    
    iSQL_List = parse(sqlst)
    #iSQL_List = gn_sqlp_cls.__sql_parsed_obj
    
    sel_obj = iSQL_List.get('select')
    selent_list = gn_sqlp_cls.gn_sqlp_select_processing(sel_obj)
    print('----------- select Processing done----')
    print(selent_list)
    print('--------------------------------------')
    #from_obj = iSQL_List.get('from')
    #(entlist, ent_alias_list, frm_str) = gn_sqlp_cls.gn_sqlp_from_processing(from_obj)                
    #print('----------from Processing done----')
    entlist = gn_sqlp_cls.get_entlist()
    ent_alias_list = gn_sqlp_cls.get_entlist_alias()
    frm_str = gn_sqlp_cls.get_from_str()
    print(entlist)
    print(ent_alias_list)
    print(frm_str)      

    print('--------where Processing ')
    where_obj = iSQL_List.get('where')
    print(where_obj)
    where_str = gn_sqlp_cls.gn_sqlp_where_processing(where_obj)
    print(where_str)
    
    ## prepare original SQL
    if (ent_alias_list[0] is not None):
        nodeid_sel = ent_alias_list[0]+"."+"gnnodeid"
    else:
        nodeid_sel = "gnnodeid"
        
    selent_list.append(nodeid_sel)
    selstr = 'select'
    first_flag = 0
    for s in selent_list:
        if (first_flag > 0):
            selstr += ","
        selstr += " "+s
        first_flag+=1
            
    print(selstr)
    print(frm_str)
    print(where_str)

    
if __name__ == "__main__":


    iSQL = """
    select s.salesorderid, s.salesdate, c.customerid, c.customername, p.productid 
    from salesorder as s JOIN customer as c 
    ON c.customerid=s.customerid 
    INNER JOIN product as p ON p.productid=s.productid AND p.productdate >2010
    where s.salesdate > 2010
"""
    gn_srch_sqlp_api(iSQL)
    
