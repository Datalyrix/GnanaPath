# GnanaPath
<h3> Graph-based framework for connected-data analytics</h3>


The framework allows you to store data into node and edges and creates edges as based on logic you can provide.
The framework has  visualization of metadata nodes and datanodes.

It is built using following 

- python-spark
- cytoscape-js
- jupyter notebook
- backend graph engine is using neo4j 
- python flask-based ui

The framework can be execute as part of docker containers

<h2>This project is still under construction Please check for updates on this page </h2>


<h3> Using Gnanapath</h3>

<h4> Setup GnanaPath </h4>
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








