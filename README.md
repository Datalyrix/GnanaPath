
<h3> Graph-based framework for connected-data analytics</h3>
- updated time Nov 1 2021

The framework allows you to store data into node and edges and creates edges as based on logic you can provide.
The framework has  visualization of metadata nodes and datanodes.

It is built using following 

- pyspark
- cytoscape-js
- jupyter notebook
- backend storage with posgres and staticfiles (stored) in container
- python flask


The framework can be execute as part of docker containers

<h2>This project is still under construction. Please check for updates on this page </h2>


<h3> Using Gnanapath</h3>

<h4> Setup using Dockerfile </h4>

 After checking out repo. Go to the local repo directory

cd GnanaPath
Choose the appropriate Dockerfiler (pyspark or Jupyter Notebook)
for pyspark-based container:

\$ ln -s Dockerfile.pyspark Dockerfile

\# run docker build and run container
# sh ./gn_docker_create_run.sh

or run step-by-step commands

\# docker build -t gnpath .

After the docker image is created run the image

#docker run -d -p 5050:5050 --name gnpath_server gnpath:latest

After that open browser http://<dockerhostip>:5050

Login/passwd: gnadmin/gnana

## To clean up container
sh ./gn_docker_cleanup.sh



##  GNGraph Storage
Currently gnpath supports storage  as static files and/or backend database using postgres

By default, the static files storage is always turned on.

To enable database mode, you will need to setup postgres database.

Under Graph Config section, you can enable static file mode and db mode and also setup database configuration.





#### Setting up Postgres container
You can run postgres as container and connect with GNPath

#docker pull postgres:alpine

# Run postgres container

#docker run --name gnpgresdb -p 5432:5432 -e POSTGRES_USER=<PostgreUser> -e POSTGRES_PASSWORD=<PASSWORD> -d postgres:alpine

 
#### Setup GNGraph DB on Postgres DB

Now login into GNPath UI

Go to "Graph Config" section. Click DB Setup

You will go to GNGraph Postges DB Settings

Enter Server IP (If you are using docker container, make sure you add host.docker.internal as Server IP or IP address of the host. if docker network is configured, you can add container name as hostname)

and other credentials (port, username, and password)

Under Database Name: You can create new Database for GNgraph or use existing database where the GNgraph schemas and tables are created.




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

- Scaling with Data
- Addign Business Data connectors




- GnanaPath Team
 

