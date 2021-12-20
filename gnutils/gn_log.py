import os
import sys
import logging
"""

  Gn Log : Main class to set logging file

"""


def     gn_logging_init(logname):

    print("GnLog: Initializing Logging ")
    path = os.getcwd()
    gnRootDir = path.rsplit('/', 1)[0]
    gnLogDir = gnRootDir+"/gnlog"

    if not os.path.isdir(gnLogDir):
        os.mkdir(gnLogDir)

    gnLogFile = "gnpath.log"
    logfilepath = gnLogDir+"/"+gnLogFile
    
    logging.basicConfig(filename=logfilepath, filemode='a', \
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',\
                       level=logging.INFO)
    logger = logging.getLogger(logname)

    return logger



def    gn_log(msg):
    gnlogger.info(msg)


def    gn_log_err(msg):
    gnlogger.error(msg)


gnlogger = gn_logging_init("GNPath")
gn_log('GNPath logging initialized')
