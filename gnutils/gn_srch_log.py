import os
import sys
import logging
"""

  Gn Search Log : Main class to set logging file

"""
gnsrch_logger = None

def     gnsrch_logging_init(logname, gnRootDir):
    global gnsrch_logger
    print("GnSrchLog: Initializing Logging ")
  
    gnLogDir = gnRootDir+"/gnlog"

    if not os.path.isdir(gnLogDir):
        os.mkdir(gnLogDir)

    gnLogFile = "gnsrch.log"
    logfilepath = gnLogDir+"/"+gnLogFile
    print('GnSrchLog: Initializing logfilepath '+logfilepath)
    
    logging.basicConfig(filename=logfilepath, filemode='a', \
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',\
                        level=logging.INFO)
    
    gnsrch_logger = logging.getLogger(logname)
    return



def    gnsrch_log(msg):
    global gnsrch_logger
    
    ###print('Got '+msg)
    if (gnsrch_logger is not None):
        gnsrch_logger.info(msg)


def    gnsrch_log_err(msg):
    global gnsrch_logger
    if (gnsrch_logger is not None):
       gnsrch_logger.error(msg)
