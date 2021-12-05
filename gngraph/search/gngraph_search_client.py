import socket
import os
import json

SEPARATOR = ","
BUFFER_SIZE = 4096
HOST="127.0.0.1"
GPORT=4141

def     gngrph_search_client_sendreq(tskcmd, targs, nodeonly):
    
    s = socket.socket()
    print(f"[+] Connecting to {HOST}:{GPORT}")
    s.connect((HOST, GPORT))
    print("[+] Connected to ", HOST)

    print(tskcmd)
    print(targs)
    t = {}
    t["cmd"] = tskcmd
    t["args"] = targs
    t["nodeonly"] = nodeonly

    print(' tobj ')
    print(t)
    tJstr = json.dumps(t)
    print(' tobj string ')
    print(tJstr)
    
    ###sndmsg = f"{tskcmd}{SEPARATOR}{targs}{SEPARATOR}{nodeonly}"

    ##s.send(sndmsg.encode())
    s.send(tJstr.encode())
    ##print(sndmsg)

    #progress = tqdm.tqdm(range(filesize), f"Sending {filename}", unit="B", unit_scale=True, unit_divisor=1024)
    
    resp = ''
    while True:
        bytes_read = s.recv(BUFFER_SIZE).decode()
        if not bytes_read:
            break
        resp += bytes_read
        #####s.sendall(bytes_read)
        #progress.update(len(bytes_read))
    s.close()
    ##print(' Received response ')
    ##print(resp)
    return resp


def      gngrph_metaqry_request(srchfilter):
    
    tskcmd="metasearch"
    #print(' task cmd ')
    #print(tskcmd)
    nodeonly = 0
    resp = gngrph_search_client_sendreq(tskcmd, srchfilter, nodeonly)
    rJData = json.loads(resp)
    rdata = rJData["data"]
  
    print(' Meta srch response ')
    #print(rdata)
    return rdata

def      gngrph_datarepo_qry_request(srchstr, nodeonly):
    
    tskcmd="datasearch"
    #print(' task cmd ')
    #print(tskcmd)
    
    resp = gngrph_search_client_sendreq(tskcmd, srchstr, nodeonly)
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
   
