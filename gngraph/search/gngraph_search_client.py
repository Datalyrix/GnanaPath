import socket
import os
import json
import psutil

from gnutils.gn_ops_cmd_utils  import GnCheckServices, GnServiceStart
from gnutils.gn_log import gn_log, gn_log_err
from gngraph.search import gngraph_search_main

##### Pyspark related variable for non-threaded spark implementation
gnp_spark = None
gnsrch_ops = None
gngraph_init_flag = 0
gnsrch_thread_flag = 0

def       __gn_graph_init(gnp_thread_flag, gnRootDir):
   global gnsrch_thread_flag
   global gnp_spark
   global gnsrch_ops
   
   if (gnp_thread_flag == 1):
       # we are spawning GnSearch Spark Application
       gngraph_search_service_init(gnRootDir)
       gngraph_init_flag = 1
       gnsrch_thread_flag = 1
       return gngraph_init_flag
   
   gn_log('GnSrchOps: Initializing Spark Session for Search '+str(gngraph_init_flag))
   if (gngraph_init_flag == 1):
       gn_log('GnAppSrv: Graph is already initialized ')
       return gngraph_init_flag
   
   app_name="gngraph_spk"
   gn_log('GnAppSrv: Initializing Spark Session ' )

   conf = SparkConf()
   conf.set('spark.executor.memory', '4g')
   conf.set('spark.driver.memory', '4g')

   gnp_spark = SparkSession \
           .builder \
           .appName("gnp") \
           .config("spark.some.config.option", "some-value") \
           .getOrCreate()

   gn_log('GnAppSrv: Spark session acquired ')
   #sc = pyspark.SparkContext(appName=app_name)
   gnp_spark.sparkContext.setLogLevel("WARN")
   
   ### Initialize GNGraph Sessions
   gn_log('GnAppSrv: Initializing Search Ops ')
   gnsrch_ops = gngrph_search_init(gnp_spark,   app.config["gnDataFolder"],  app.config["gnGraphDBCredsFolder"],  app.config["gnCfgSettings"])
   gngraph_init_flag = 1
   gn_log('GnAppSrv: gngraph Init COMPLETE SUCCESS '+str(gngraph_init_flag))
   return gngraph_init_flag


SEPARATOR = ","
BUFFER_SIZE = 4096
HOST="127.0.0.1"
GPORT=4141

def     gngrph_search_client_sendreq(t):
    
    s = socket.socket()
    
    try:
        s.connect((HOST, GPORT))
        gn_log("GnSrchOps: connected to srch service:"+HOST)        
        tJstr = json.dumps(t)
        gn_log("GnSrchOps: send request: "+str(tJstr))

        s.send(tJstr.encode())
    
        resp = ''
        
        while True:
           bytes_read = s.recv(BUFFER_SIZE).decode()
           if not bytes_read:
               break
           resp += bytes_read

        s.close()
        rJ = json.loads(resp)
        gn_log("GnSrchOps: recv resp cmd "+rJ["cmd"]+"  status "+rJ["status"])
        
    except socket.error as err:
        gn_log('GnSrchOps: internal socket error '+str(err))
        resp_j = {}
        resp_j["cmd"] = t["cmd"]
        resp_j["status"] = "ERROR"
        rJ = {}
        rJ["nodes"] = []
        rJ["edges"] = []
        rJ["nodelen"] = 0
        rJ["edgelen"] = 0
        rJ["status"] = "ERROR"
        rJ["errmsg"] = "Internal Server Error"
        resp_j["data"] = rJ
        resp = json.dumps(resp_j)
        gn_log('GnSrchOps: cmd '+rJ["cmd"]+' failed to execute ')
        
    return resp

def      gngrph_metanodes_get_request():
    
    tskcmd="metanodes"
    #print(' task cmd ')
    #print(tskcmd)
    srchfilter = ""
    nodeonly = 0
    t = {}
    t["cmd"] = "metanodes"
    t["args"] = ""
    t["nodeonly"] = nodeonly

    resp = gngrph_search_client_sendreq(t)
    
    rJData = json.loads(resp)
    rdata = rJData["data"]
  
    print('GnSrchReq:  get Meta nodes response ')
    print(rJData)
    return rJData


def      gngrph_meta_remap_request():
    
    tskcmd="metaremap"
    #print(' task cmd ')
    #print(tskcmd)
    srchfilter = ""
    nodemode = 2
    t = {}
    t["cmd"] = "metaremap"
    t["args"] = ""
    t["nodemode"] = nodemode

    resp = gngrph_search_client_sendreq(t)
    
    rJData = json.loads(resp)
    rdata = rJData["data"]
  
    gn_log('GnSrchOps:  metarepo remap request response ')
    gn_log(rJData)
    return rJData 
 
def      gngrph_metaqry_request(srchfilter):
    
    tskcmd="metasearch"
    #print(' task cmd ')
    #print(tskcmd)
    nodeonly = 0
    t = {}
    t["cmd"] = "metasearch"
    t["args"] = srchfilter
    t["nodeonly"] = nodeonly

    resp = gngrph_search_client_sendreq(t)

    rJData = json.loads(resp)
    rdata = rJData["data"]
  
    print(' Meta srch response ')
    #print(rdata)
    return rdata

def      gngrph_datarepo_qry_request(srchstr, nodemode, lnodes):
        
    t = {}
    t["cmd"] = "datasearch"
    t["args"] = srchstr
    t["nodemode"] = nodemode
    t["lnodes"] = lnodes
        
    resp = gngrph_search_client_sendreq(t)
    rJData = json.loads(resp)
    rdata = rJData["data"]
  
    print(' Data repo srch qry response ')
    #print(rdata)    
    return rdata


def        gngraph_search_check_service(gnRootDir):

    (appsrvid, srchpid) = GnCheckServices(gnRootDir)

    if (srchpid == 0):
        gn_log('GnSrchOps: search service is not running ')
        return 0
    else:
        return 1


def       gngraph_search_service_init(gnRootDir):

    servname="gnsearch"

    # check gnsearch service if it is already running
    srchpid = gngraph_search_check_service(gnRootDir)

    if (srchpid == 0):
        # create search service
        srchpid = GnServiceStart(gnRootDir, servname)
        
        if (srchpid == -1):
            gn_log('GnSrchOps: Search service failed to start ')
            return -1
        gn_log('GnSrchOps: Search service is running pid '+str(srchpid))
        return srchpid
    else:
        gn_log('GnSrchOps: Search service is already runnig pid '+str(srchpid))
        return srchpid
     


def      gnsrch_metaqry_request(srchfilter):

    if (gnsrch_thread_flag == 1):
        res = gngrph_metaqry_request(srchfilter)
    else:
        res = gngraph_search_main.gngrph_srch_metarepo_qry_fetch_api(gnsrch_ops, gnp_spark, srchfilter)

    return res


def     gnsrch_metanodes_request():

   if (gnsrch_thread_flag == 1):
       res = gngrph_metanodes_get_request()
   else:
       res = gngraph_search_main.gngrph_srch_metarepo_qry_fetch_nodes_api(gnsrch_ops, gnp_spark, srchfilter)
   return res

def    gnsrch_meta_remap_request():

    if (gnsrch_thread_flag == 1):
        res = gngrph_meta_remap_request()
        return res    
    

def    gnsrch_dataqry_request(srchfilter, nodemode, lnodes):

    if (gnsrch_thread_flag == 1):
        res = gngrph_datarepo_qry_request(srchfilter, nodemode, lnodes)

    else:
        res = gngraph_search_main.gngrph_srch_datarepo_qry_fetch_api(gnsrch_ops, gnp_spark, srchfilter, nodemode)

    return res



if __name__ == "__main__":
    
    print(' Starting gngraph client ')
    srchfilter="SELECT * from Customer LIMIT 10000"
    ##gngrph_metaqry_request(srchfilter)
    nodesonly = 0
    gngrph_datarepo_qry_request(srchfilter, nodesonly)
   
