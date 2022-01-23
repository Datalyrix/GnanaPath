#!/bin/bash

echo "GnPath  Debug Mode:$GN_DEBUG_MODE"

if [ ! -z "$GN_DEBUG_MODE" ]
then
    echo " Running in command debug mode "
    /bin/bash
fi

echo "Starting GnPath Application Server"
cd /opt/GnanaPath/gnappsrv
nohup python gnp_appsrv_main.py
