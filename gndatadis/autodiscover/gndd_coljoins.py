import os
from os import path
import sys
import numpy as np
import pandas as pd
import chardet
import json 

"""
gndd_coljoins: Module to autodiscover the columns in multiple files for potential join condition

"""

curDir = os.getcwd()
lvDir = curDir.rsplit('/', 1)[0]
rtDir = lvDir.rsplit('/', 1)[0]

if lvDir not in sys.path:
   sys.path.append(lvDir)

if rtDir not in sys.path:
   sys.path.append(rtDir)

print(lvDir)
print(rtDir)

import gnappsrv.gn_config as gnconfig



def    similarColumnsCheck_fn(df1, df2, df1name, df2name, rfp, fp):
    
   df1size = len(df1)
   df1DTypeDict = dict(df1.dtypes)
   df2size = len(df2)
   df2DTypeDict = dict(df2.dtypes)


   df1ColList = df1.columns
   df2ColList = df2.columns
    
   #### Get similar collist
   similarCol = []
    
   for  c in df1ColList: 
    
       cdtype = df1DTypeDict[c]
       if (fp): 
           fp.write("############ "+df1name+" Column "+c+"  datatype "+str(cdtype)+"#############\n") 
       else:
           print("############ "+df1name+" Column "+c+"  datatype "+str(cdtype)+"#############") 
    
    
       for d in df2ColList:
            ddtype = df2DTypeDict[d]
            if (fp):
               fp.write("################### "+df2name+"  Column "+d+"  datatype "+str(ddtype)+"#############\n")
            else: 
               print("################## "+df2name+" Column "+d+"  datatype "+str(ddtype)+"#############")
                    
            if (str(cdtype) != str(ddtype)):
                if (fp):
                   fp.write("##########################  Columns("+df1name+"."+c+","+df2name+"."+d+") datatypes do not match\n")
                else:               
                   print("##########################  Columns("+df1name+"."+c+","+df2name+"."+d+") datatypes do not match")
                continue
            
            ### Let us restrict to only int64
            #if ((str(cdtype) != 'int64') or (str(cdtype) != 'float64')):
            #    print("##########################  Columns("+c+","+d+") datatypes are not integer "+str(cdtype))
            #    continue
            
            cdf_n = df1.filter(items=[c],axis=1)
            cdf = cdf_n[cdf_n[c].notnull()]
            clist = cdf[c].sort_values().unique()
            ccount = len(clist)
   

            ddf_n = df2.filter(items=[d], axis=1)
            ddf  = ddf_n[ddf_n[d].notnull()]
            dlist = ddf[d].sort_values().unique()
            dcount= len(dlist)
            
            if (ccount >= dcount):
                cset = np.isin(dlist, clist)
                ###sres = cdf[c].isin(ddf[d]).value_counts()
                sres = pd.Series(cset).value_counts()
                ncount = dcount    
            else:
                dset = np.isin(clist, dlist)
                ####sres = ddf[d].isin(cdf[c]).value_counts()
                sres = pd.Series(dset).value_counts()
                ncount = ccount
             
            
            if (ncount == 0):
                continue
                
            #sres = bigdf[c].isin(smdf[d]).value_counts()
            if (fp):
               fp.write(' DF1 '+df1name+' col: '+c+'  unique_count:'+str(ccount)+"\n")
               fp.write(' DF2 '+df2name+' col: '+d+'  unique_count:'+str(dcount)+"\n")
               fp.write("###################  Columns("+c+","+d+")  match check \n")
            else:
               print(' DF1 '+df1name+' col: '+c+'  unique_count:'+str(ccount))
               print(' DF2 '+df2name+' col: '+d+'  unique_count:'+str(dcount))
               print("###################  Columns("+c+","+d+")  match check")
            
            simcount = 0
            nsimcount = 0
            for i in sres.index:
               if (i == False):
                 nsimcount = sres[i]
               if (i == True):
                 simcount = sres[i]
            if (fp):        
              fp.write("##########################  SimilarCount "+np.int64(simcount).astype(str)+"\n")
              fp.write("##########################  NonSimilarCount "+np.int64(nsimcount).astype(str)+"\n")
              fp.write("##########################     totalcount "+str(ncount)+"\n")    
            else:
              print("##########################  SimilarCount "+numpy.int64(simcount).astype(str))
              print("##########################  NonSimilarCount "+numpy.int64(nsimcount).astype(str))
              print("##########################     totalcount "+str(ncount))       
              
            simper = (simcount)/ncount                
            simper_str = np.float64(simper).astype(str)
            
            if (fp):
              fp.write("##########################  SimilarPer "+simper_str+"\n")
            else:
              print("##########################  SimilarPer "+simper_str)
            
            if (simper > 0.0):
               similarCol.append([c, d, simper_str])
            
   #### Write results to file
   if(fp):
     fp.write("###################################################\n")
     fp.write("##############Results##############################\n")
     for listitem in similarCol:
         fp.write('%s\n' % listitem)

  
   scol_jstr = []
   for item in similarCol:
      jstr = {}
      c1 = item[0]
      c2 = item[1]
      sper = item[2]
      jstr["col1"] = c1
      jstr["col2"] = c2
      jstr["sper"] = sper
      scol_jstr.append(jstr)
   jsonStr = json.dumps(scol_jstr)
   if (rfp):
        rfp.write(jsonStr)
            
            
   return jsonStr 

def     create_pdf(filepath):  
  
  with open(filepath, 'rb') as rawdata:
    sresult = chardet.detect(rawdata.read(100000))
  
  print(sresult['encoding'])
  
  fp_df =  pd.read_csv(filepath, sep='|', encoding=sresult['encoding'])
  
  return(fp_df)



def      gndd_columnjoin_discovery_api(fp1, fp2, tgtfolder):
    
    f1name = fp1.rsplit('/',  1)[1]
    f2name = fp2.rsplit('/',  1)[1]
    
    f1_pdf = create_pdf(fp1)
    f2_pdf = create_pdf(fp2)
    
    logfile = tgtfolder+"/"+f1name+"_"+f2name+"_discover.log"
    resfile = tgtfolder+"/"+f1name+"_"+f2name+"_discover_res.json"
    logfp = open(logfile, "w")
    resfp = open(resfile, "w")
    
    f1_pdf = create_pdf(fp1)
    f2_pdf = create_pdf(fp2)
    
    scol_list = similarColumnsCheck_fn(f1_pdf, f2_pdf, f1name, f2name, resfp, logfp)
    resfp.close()
    logfp.close()

if __name__ == "__main__":
    
     
     sfile_name = "Sales.csv"   
     sfile = gnconfig.GN_DATA_UPLOADS_FOLDER+"/"+sfile_name 

     cfile_name = "Customer.csv"
     cfile = gnconfig.GN_DATA_UPLOADS_FOLDER+"/"+cfile_name
     
     tgtfolder = gnconfig.GN_DATA_DISCOVER_FOLDER
     gndd_columnjoin_discovery_api(sfile, cfile, tgtfolder)

        

    
