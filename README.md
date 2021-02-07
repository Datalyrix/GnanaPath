
<h3> Graph-based framework for connected-data analytics</h3>
- updated time Feb6 2021

The framework allows you to store data into node and edges and creates edges as based on logic you can provide.
The framework has  visualization of metadata nodes and datanodes.

It is built using following 

- pyspark
- cytoscape-js
- jupyter notebook
- backend graph engine is using neo4j 
- python flask

The framework can be execute as part of docker containers

<h2>This project is still under construction. Please check for updates on this page </h2>


<h3> Using Gnanapath</h3>

<h4> Setup using Dockerfile </h4>

After checking out repo. Go to the local repo directory
cd GnanaPath

\# run docker build
\# docker build -t gnpath .

After the docker image is created run the image

#docker run -d -p 5050:5050 --name gnpath_server gnpath:latest

After that open browser http://<dockerhostip>:5050

Login/passwd: gnadmin/gnana

 You can continue from "Connecting to backend Neo4j Server" section below

 



<h4> Setup GnanaPath Manually</h4>
We built using jupyter/pyspark notebook.  

- Download and install jupyter/pyspark notebook or get docker container from https://hub.docker.com/r/jupyter/pyspark-notebook/

- If you choose docker container for notebook, then make sure you port map for 5050 for Gnana UI
 Example:
  \# docker run -d -p 8888:8888  <b>-p 5050:5050</b>  --name jupyntbook  jupyter/pyspark-notebook

- Open terminal on the notebook and git clone GnanaPath repo

 \# git clone https://github.com/Datalyrix/GnanaPath.git

-  Install python related modules for this project

\# pip install -r gnpappsrv/requirements.txt


<h4> Neo4j Setup and Server Credentials </h4>
We are neo4j as backend graph engine to store nodes and edges. We use python-bolt driver to update neo4j engine.
If you already have neo4j installed and up and running, then you can skip this step.

We have used community edition of neo4j container to setup. For other options please visit http://neo4j.com

Please refer link to install neo4j as container https://github.com/neo4j/docker-neo4j

get Bolt port (ex: 7687) and user credentials (user/passw) for GnanaUI to connect


<h4> Starting GnanaApp Server </h4>
Now you are ready to run GnanaApp Server. Goto gnana clone repo directory

cd gnpappsrv

python gnpappsrv

You should see log like:
* Serving Flask app "gnp_appsrv_main" (lazy loading)
 * Environment: production
   WARNING: This is a development server. Do not use it in a production deployment.
   Use a production WSGI server instead.
 * Debug mode: on
 * Running on http://0.0.0.0:5050/ (Press CTRL+C to quit)
 * Restarting with stat
 * Debugger is active!
 * Debugger PIN: XXX-XXX-XXX
 
 Now open browser go to  http://<jupyternotebook-ip>:5050
 You can GnanaPath UI and click login
 Default user/passwd  (gnadmin/gnana)
 
 
 <h4> Connecting to backend Neo4j Server </h4>
You will have connect to backend neo4j server to store data.

After you login into GnanaPath UI, 
 
- click connect

- Enter Neo4j Server IP and Bolt Protocol port(ex: 7687)
   Ex:  XXX.XXX.XXX.XXX:7687
   
- Enter username and password
Click connect, if you connection is successful you will see success status message


***Now you are ready to upload the data*****

 <h4> Upload data </h4>
 
 Currently, we support simple file upload using csv or json. we *donot* yet support nested json files.
 
 - Click Upload on top menu options on GnanaPath UI
 
 - Upload file from local file path
 
 - After upload is complete, you will see upload success message
 
 *note*  currently csv file header line (line 1) is treated as meta data header.
 
 <h4> View the data in graph </h4>
 
 Click  MetaView on top men option to view meta nodes created from file upload and SearchView to view data nodes.
 
 
 <h2> ToDo list </h2>

We have lot of todo list for this project and will be updating in this section.

- pyspark-based graph engine




- GnanaPath Team
 

