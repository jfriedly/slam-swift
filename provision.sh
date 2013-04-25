#!/bin/bash

sudo apt-get install -y python-pip
sudo pip install -U python-keystoneclient python-swiftclient

source openrc.sh
python slam.py --workers=4 --objects 1024 --object-size 1
