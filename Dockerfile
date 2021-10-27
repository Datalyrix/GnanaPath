##ARG BASE_CONTAINER=jupyter/pyspark-notebook
FROM jupyter/pyspark-notebook

LABEL maintainer="gnanpath project"
EXPOSE 5050
USER root

WORKDIR /opt/GnanaPath
COPY gnappsrv/ gnappsrv/
COPY gngraph/ gngraph/
COPY gndatadis/ gndatadis/
COPY gnsampledata/ gnsampledata/
COPY gntests/ gntests/
COPY __init__.py __init__.py
COPY README.md README.md
COPY gndwdb/ gndwdb/
COPY gnsearch/ gnsearch/
COPY gnutils/  gnutils/
COPY creds/ creds/
COPY Jenkinsfile Jenkinsfile
COPY startgnpath.sh startgnpath.sh
WORKDIR gnappsrv/

###psycopg2 cannot be pip installed
RUN conda install -y psycopg2
RUN pip install -r requirements.txt

#RUN useradd gnuser && chown -R gnuser /opt/GnanaPath

RUN mkdir /opt/GnanaPath/tmp
WORKDIR /opt/GnanaPath/gnappsrv

#RUN nohup python gnp_appsrv_main.py &
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/GnanaPath/startgnpath.sh"]
WORKDIR $HOME