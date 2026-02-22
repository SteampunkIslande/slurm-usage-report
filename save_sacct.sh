#!/bin/bash

# Daily script to save yesterday's performances

yesterday=$(date -d 'yesterday' '+%Y-%m-%d')
yesterday_start=$(date -d 'yesterday' '+%Y-%m-%dT00:00:00')
yesterday_end=$(date -d 'yesterday' '+%Y-%m-%dT23:59:59')

sacct -a -P -o 'ALL' -S $yesterday_start -E $yesterday_end > SACCT_DB/$yesterday.csv

