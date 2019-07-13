#!/usr/bin/env bash

sudo apt-get update -qq
sudo apt-get install libsnappy-dev zlib1g-dev libbz2-dev -qq

git clone https://github.com/facebook/rocksdb.git rocksdb
cd rocksdb
git reset --hard v5.18.3
make shared_lib
