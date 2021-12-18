import psutil
import os
import sys
import time
import logging

##import subprocess

curentDir = os.getcwd()
gnRootDir = curentDir.rsplit('/', 1)[0]
if gnRootDir not in sys.path:
    sys.path.append(gnRootDir)
    
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
           print(' '+processName+' found with pid '+str(pid)+' ')
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

def    GnCheckServices():
    # Check if any chrome process was running or not.
    
    srchpid = checkIfProcessRunning(GNSRCH_APP)
    appsrvpid = checkIfProcessRunning(GNAPP_SRV)
    
    if (srchpid == 0):
        print('Search service is not running ') 
    else:
        print('Search is running with pid '+str(srchpid))

    if (appsrvpid == 0):
        print('App Service is not running ')
    else:
        print('App Service is running with pid '+str(appsrvpid))


def     GnServiceStart(servname):

    if (servname == "gnsearch"):
        srchpid = checkIfProcessRunning(GNSRCH_APP)
        if (srchpid != 0):
            print('GnSearch service is already running')
            return 0
        else:
            cm = gnRootDir+"/gngraph/search/startGNSearch.sh "+gnRootDir
            #rc = subprocess.call(cm)
            rc = os.system(cm)
            time.sleep(5)
            # check if that process got killed 
            srchpid1 = checkIfProcessRunning(GNSRCH_APP)
            if (srchpid1 != 0):
                print('GnSearch service is started succesfully ')
            else:
                print('GnSearch service failed to start ')

    if (servname == "gnappsrv"):
        srvpid = checkIfProcessRunning(GNAPP_SRV)
        if (srvpid != 0):
            print('GnOps: GnApp Server service is already running')
            return 0
        else:
            cm = gnRootDir+"/gnappsrv/startGNAppSrv.sh "+gnRootDir
            #rc = subprocess.call(cm)
            rc = os.system(cm)
            time.sleep(5)
            # check if that process got killed 
            srchpid1 = checkIfProcessRunning(GNAPP_SRV)
            if (srchpid1 != 0):
                print('GnOps: GnApp Server service is started succesfully ')
            else:
                print('GnOps: GnApp Server service failed to start ')
   
                
        
def     GnServiceStop(servname):

    logFile=gnRootDir+"/gnlog/gnops.log"
    if (servname == "gnsearch"):
        srchpid = checkIfProcessRunning(GNSRCH_APP)
        if (srchpid == 0):
            msg="GnOps: search service is not running"
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
                print('GnSearch service is stopped succesfully ')
            else:
                print('GnSearch service failed to stop ')
   
    if (servname == "gnappsrv"):
        srvpid = checkIfProcessRunning(GNAPP_SRV)
        if (srvpid == 0):
            msg="GnOps: GnApp Server service is not running"
            print(msg)
            os.system('echo "`date`: '+msg+' " >> '+logFile+'  ')
            return 0
        else:
            msg="GnOps: GnApp Server stopped the services"
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
    
    #print(' Root Director '+gnRootDir)
    ##parser = argparse.ArgumentParser()
    nargs = len(sys.argv)
    #print('sys argc '+str(nargs))
    
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
        GnCheckServices()
    elif (cmd == "start"):    
        if (servname != 'gnappsrv') and (servname != 'gnsearch'):
            print('GnOps: Invalid service name allowed services: gnappsrv, gnsearch ')
            sys.exit(0)
        print('GnOps: start '+servname+' service ')
        GnServiceStart(servname)
    elif (cmd == "stop"):
        if (servname != 'gnappsrv') or (servname != 'gnsearch'):
            print('GnOps: Invalid service name allowed services: gnappsrv, gnsearch ')
            sys.exit(0)
        
        print('GnOps: stop '+servname+' service ')
        GnServiceStop(servname)
    else:
        print('GnOps: Invalid command string: allowed cmds:check, start, stop ')
