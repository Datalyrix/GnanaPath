import psutil
import os
import sys
import time
import logging

##import subprocess
curentDir = os.getcwd()
GnRootDir = curentDir.rsplit('/', 1)[0]
if GnRootDir not in sys.path:
    sys.path.append(GnRootDir)
    
GNSRCH_APP="gngraph_search_main.py"
GNAPP_SRV="gnp_appsrv_main.py"

    

def   checkIfProcessRunning(processName):
    '''
    Check if there is any running process that contains the given name processName.
    '''
    
    
    L=[(p.name(),p.pid, p.cmdline()[1]) for p in psutil.process_iter() if p.name().startswith('py')]
    ####print(L)
    for x in L:
       (name, pid, cmdline) = (x)
       #print('Name '+str(name)+" pid "+str(pid)+"  cmdline "+cmdline)
       
       if processName in cmdline:
           ##print(' '+processName+' found with pid '+str(pid)+' ')
           return pid
    pid = 0   
    print('processName '+processName+' is not running ')   
    #Iterate over the all the running process
    #for proc in psutil.process_iter():
    #    try:
    #        print(' proc name '+proc.name().lower())
    #        print(proc.pid)
    #        #print(' proce cmdline ')
            #print(proc.cmdline)
            # Check if process name contains the given name string.
    #        if processName.lower() in proc.name().lower():
    #            return True
    #    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
    #        pass
    return pid;


def      GnCheckServices(gnRootDir):
    # Check if any chrome process was running or not.
    
    srchpid = checkIfProcessRunning(GNSRCH_APP)
    appsrvpid = checkIfProcessRunning(GNAPP_SRV)
    
    if (srchpid == 0):
        print('GnSearch service is STOPPED ') 
    else:
        print('GnSearch is RUNNING pid '+str(srchpid))

    if (appsrvpid == 0):
        print('GnAppServer is STOPPED ')
    else:
        print('GnAppServer is RUNNING pid '+str(appsrvpid))
        
    return(appsrvpid, srchpid)

def     GnServiceStart(gnRootDir, servname):

    if (servname == "gnsearch"):
        srchpid = checkIfProcessRunning(GNSRCH_APP)
        if (srchpid != 0):
            print('GnSearch is already RUNNING pid '+str(srchpid))
            return srchpid
        else:
            cm = gnRootDir+"/gngraph/search/startGNSearch.sh "+gnRootDir
            #rc = subprocess.call(cm)
            rc = os.system(cm)
            time.sleep(5)
            # check if that process got killed 
            srchpid1 = checkIfProcessRunning(GNSRCH_APP)
            if (srchpid1 != 0):
                print('GnSearch is successfully RUNNING pid '+str(srchpid1))
            else:
                print('GnSearch is failed to start  STOPPED ')
                return -1
                
    if (servname == "gnappsrv"):
        srvpid = checkIfProcessRunning(GNAPP_SRV)
        if (srvpid != 0):
            print('GnAppServer is already RUNNING pid '+str(srvpid))
            return 0
        else:
            cm = gnRootDir+"/gnappsrv/startGNAppSrv.sh "+gnRootDir
            #rc = subprocess.call(cm)
            rc = os.system(cm)
            time.sleep(5)
            # check if that process got killed 
            srchpid1 = checkIfProcessRunning(GNAPP_SRV)
            if (srchpid1 != 0):
                print('GnAppServer service is succesfully RUNNING pid '+str(srchpid1))
            else:
                print('GnAppServer service failed to start ')
   
                
        
def     GnServiceStop(gnRootDir, servname):

    logFile=gnRootDir+"/gnlog/gnops.log"
    if (servname == "gnsearch"):
        srchpid = checkIfProcessRunning(GNSRCH_APP)
        if (srchpid == 0):
            msg="GnSearch service is STOPPED"
            print(msg)
            os.system('echo "`date`: '+msg+' " >> '+logFile+'  ')
            return 0
        else:
            msg="GnOps: stopped the services"
            cm = "kill "+str(srchpid)
            #rc = subprocess.call(cm)
            rc = os.system(cm)
            os.system('echo "`date`: '+msg+' " >> '+logFile+'  ')
            time.sleep(4)
            # check if that process got killed 
            srchpid1 = checkIfProcessRunning(GNSRCH_APP)
            if (srchpid1 == 0):
                print('GnSearch service is succesfully STOPPED')
            else:
                print('GnSearch service failed to stop RUNNING ')
   
    if (servname == "gnappsrv"):
        srvpid = checkIfProcessRunning(GNAPP_SRV)
        if (srvpid == 0):
            msg="GnAppServer service is STOPPED"
            print(msg)
            os.system('echo "`date`: '+msg+' " >> '+logFile+'  ')
            return 0
        else:
            msg="GnAppServer stopping the services"
            cm = "kill "+str(srvpid)
            #rc = subprocess.call(cm)
            rc = os.system(cm)
            os.system('echo "`date`: '+msg+' " >> '+logFile+'  ')
            time.sleep(4)
            # check if that process got killed 
            srchpid1 = checkIfProcessRunning(GNAPP_SRV)
            if (srchpid1 == 0):
                print('GnOps: GnApp Server service is stopped succesfully ')
            else:
                print('GnOps: GnApp Server service failed to stop ')
   

                
if  __name__ == "__main__":
    
    nargs = len(sys.argv)
    
    if nargs  < 2:
        print('gn_ops_cmd_utils <cmd> <servname>')
        sys.exit(0)
        
    if nargs == 2:     
        (prog, cmd) = sys.argv
        servname=''
    else:
        (prog,cmd,servname) = sys.argv
      
    if cmd is None:
        print('gn_ops_cmd_utils <cmd> <servname>')

    ##print(prog)    
    #servname 
    #print(servname)
    
    if (cmd == "check"):
        print('GnOps: checking for services')
        (appsrvpid, srchpid) = GnCheckServices(GnRootDir)
    elif (cmd == "start"):    
        if (servname != 'gnappsrv') and (servname != 'gnsearch'):
            print('GnOps: Invalid service name allowed services: gnappsrv, gnsearch ')
            sys.exit(0)
        print('GnOps: start '+servname+' service ')
        GnServiceStart(GnRootDir, servname)
    elif (cmd == "stop"):
        if (servname != 'gnappsrv') and (servname != 'gnsearch'):
            print('GnOps: Invalid service name allowed services: gnappsrv, gnsearch ')
            sys.exit(0)
        
        print('GnOps: stop '+servname+' service ')
        GnServiceStop(GnRootDir, servname)
    else:
        print('GnOps: Invalid command string: allowed cmds:check, start, stop ')
