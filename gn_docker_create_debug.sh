#!/bin/bash

docker build -t gnp_dbg .

docker run -it --name gnp_dbg_c -v `pwd`:/opt/gnpdev -p 5050:5050 -p 4040:4040 gnp_dbg
#docker run -it --name gnp_test -v ./:/opt/GNPathDev -p 5050:5050 gnp
