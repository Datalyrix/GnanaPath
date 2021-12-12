import socket
import os
import json

SEPARATOR = ","
BUFFER_SIZE = 4096
HOST="127.0.0.1"
GPORT=4141

def     gngrph_search_client_sendreq(t):
    
    s = socket.socket()
    print(f"[+] Connecting to {HOST}:{GPORT}")
    try:
        s.connect((HOST, GPORT))
        print("[+] Connected to ", HOST)

        #print(tskcmd)
        #print(targs)
        #t = {}
        #t["cmd"] = tskcmd
        #t["args"] = targs
        #t["nodeonly"] = nodeonly

        print(' tobj ')
        print(t)
        tJstr = json.dumps(t)
        print(' tobj string ')
        print(tJstr)
    
        ###sndmsg = f"{tskcmd}{SEPARATOR}{targs}{SEPARATOR}{nodeonly}"

        ##s.send(sndmsg.encode())
        s.send(tJstr.encode())
    
        resp = ''
        
        while True:
           bytes_read = s.recv(BUFFER_SIZE).decode()
           if not bytes_read:
               break
           resp += bytes_read

        s.close()
        ##print(' Received response ')
        ##print(resp)
    except socket.error as err:
        print(' Gngrph socket client error '+str(err))
        resp_j = {}
        resp_j["cmd"] = tskcmd
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

if   __name__ == "__main__":
    
    print(' Starting gngraph client ')
    srchfilter="SELECT * from Customer LIMIT 10000"
    ##gngrph_metaqry_request(srchfilter)
    nodesonly = 0
    gngrph_datarepo_qry_request(srchfilter, nodesonly)
   
