#!/bin/bash

docker build -t gnp .

docker run -it --name gnp_test -v `pwd`:/opt/gnpdev -p 5050:5050 gnp
#docker run -it --name gnp_test -v ./:/opt/GNPathDev -p 5050:5050 gnp
