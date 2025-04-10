#!/bin/bash

mkdir ./data
mkdir ./AIS

# Get 2022 AIS data
# https://marinecadastre.gov/ais/
wget -r -np -l 1 -A zip -P ./data https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/index.html

# Move Files
mv ./data/coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/* ./data/
rm -rf ./data/coast.noaa.gov

# Unpack
for i in ./data/*.zip; 
do
    unzip $i -d ./data
    rm $i
done

# Convert to parquet
eval "$(conda shell.bash hook)"                                                 
conda activate duckdb 
export JAVA_HOME=~/.my_conda/envs/duckdb
java --version
python convert.py

mv ./AIS/data/* ./data
rm -rf ./AIS
