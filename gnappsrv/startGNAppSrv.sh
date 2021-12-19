#!/bin/bash
ROOTDIR=$1
cd ${ROOTDIR}/gnappsrv
WORKDIR=`pwd`
GNAPP_SERV="gnp_appsrv_main.py"
LOGFILE="${ROOTDIR}/gnlog/gnpath.log"
echo "`date` starting App Server on ${WORKDIR}  "
python ${GNAPP_SERV} >> ${LOGFILE} 2>&1 & 
GNAPPSERV_PID=$!
echo "`date`  App Server service started with ${GNAPPSERV_PID} pid " 
