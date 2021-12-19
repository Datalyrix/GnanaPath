#!/bin/bash
ROOTDIR=$1
cd ${ROOTDIR}/gngraph/search
WORKDIR=`pwd`
GNSRCH_SERVICE="gngraph_search_main.py"
LOGFILE="${ROOTDIR}/gnlog/gnsrch.log"
echo "`date` starting Search on ${WORKDIR}  with logfile ${LOGFILE}" >> ${LOGFILE}
python ${GNSRCH_SERVICE}  >> ${LOGFILE} 2>&1 &
GNSRCH_PID=$!
echo "`date`  Search service started with ${GNSRCH_PID} pid " >> ${LOGFILE}
