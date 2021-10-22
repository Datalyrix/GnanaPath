import os
import logging

"""
 gn_config : sets the directory and sets the config dir config vars
"""

def        gn_config_dirs(app):
    app.config["gnDataFolder"] = app.config["gnRootDir"]+"/gndata"
    app.config["gnDBFolder"] = app.config["gnRootDir"]+"/gndb"
    app.config["gnCfgDBFolder"] = app.config["gnRootDir"]+"/gnconfigdb"
    app.config["gnUploadsFolder"] = app.config["gnDataFolder"]+"/uploads"
    app.config["gnDiscoveryFolder"] = app.config["gnDataFolder"] +"/datadiscovery"
    app.config["gnProfileFolder"] = app.config["gnDiscoveryFolder"]+"/profile"
    app.config["gnGraphFolder"] = app.config["gnDataFolder"]+ "/gngraph"
    app.config["gnGraphDBCredsFolder"] = app.config["gnRootDir"]+"/creds/gngraph"  
    
    if not os.path.isdir(app.config["gnDataFolder"]):
        os.mkdir(app.config["gnDataFolder"])
        print(app.config["gnDataFolder"]+" is created")
    if not os.path.isdir(app.config["gnUploadsFolder"]):
        os.mkdir(app.config["gnUploadsFolder"])
        print(app.config["gnUploadsDir"]+" is created")
    if not os.path.isdir(app.config["gnCfgDBFolder"]):
        os.mkdir(app.config["gnCfgDBFolder"])
        print(app.config["gnCfgDBFolder"]+" is created")    
    if not os.path.isdir(app.config["gnDiscoveryFolder"]):
        os.mkdir(app.config["gnDiscoveryFolder"])
        print(app.config["appDiscoveryFolder"]+" is created ")
    if not os.path.isdir(app.config["gnProfileFolder"]):
        os.mkdir(app.config["gnProfileFolder"])
        print(app.config["gnProfileFolder"]+" is created")
    if not os.path.isdir(app.config["gnGraphFolder"]):
        os.mkdir(app.config["gnGraphDir"])
        print(app.config["gnGraphDir"]+" is created")

        
def     gn_logging_init(app, logname):

    app.config["gnLogDir"] = app.config["gnRootDir"]+"/gnlog"

    if not os.path.isdir(app.config["gnLogDir"]):
        os.mkdir(app.config["gnLogDir"])

    app.config["gnLogFile"] = "gnpath.log"
    logfilepath = app.config["gnLogDir"]+"/"+app.config["gnLogFile"]     
    logging.basicConfig(filename=logfilepath, filemode='a', \
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',\
                        level=logging.INFO)
    logger = logging.getLogger(logname)
    return logger
