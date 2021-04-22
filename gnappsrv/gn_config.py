import sys
import os
import logging

##lvDir = os.getcwd()
##rootDir = lvDir.rsplit('/', 1)[0]
##print("gnConfig dir "+lvDir)
##print("gnConfig root dir: "+rootDir)

def        gn_config_dirs(app):
     
   ##GN_DATA_DIRPATH=app.config["gnRootDir"]
   app.config["gnDataFolder"] = app.config["gnRootDir"]+"/gndata"
   app.config["gnUploadsFolder"] = app.config["gnDataFolder"]+"/uploads" 
   app.config["gnDiscoveryFolder"] = app.config["gnDataFolder"] +"/datadiscovery"
   app.config["gnProfileFolder"] = app.config["gnDiscoveryFolder"]+"/profile"
   app.config["gnGraphFolder"] = app.config["gnDataFolder"]+ "/gngraph"
   
   
   if not os.path.isdir(app.config["gnDataFolder"]):
      os.mkdir(app.config["gnDataFolder"])
      print(app.config["gnDataFolder"]+" is created")
    
   if not os.path.isdir(app.config["gnUploadsFolder"]):
      os.mkdir(app.config["gnUploadsFolder"])
      print(app.config["gnUploadsDir"]+" is created")
    
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
      
      logging.basicConfig(filename=logfilepath, filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
      logger = logging.getLogger(logname)

      return logger
