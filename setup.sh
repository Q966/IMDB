#!/bin/bash

mkdir test_quentin
wget -i url_files.txt
gzip -dvkr title.basics.tsv.gz title.principals.tsv.gz title.ratings.tsv.gz test_quentin
conda env update --file env.yml
mysql -h {YOUR_HOST} -u {USERNAME} -p {DATABASE_NAME} < ddl.sql