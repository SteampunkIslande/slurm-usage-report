#!/bin/bash 
# Daily script to save yesterday's performances

cd $(dirname $0)

yesterday=$(date -d 'yesterday' '+%Y-%m-%d')

f=SACCT_DB/$yesterday.csv

sacct -a -P -o 'ALL' -S "${yesterday}T00:00:00" -E "${yesterday}T23:59:59" > $f

singularity run -B /tmp /SINGULARITIES/slurm-usage-report-1.0.0.sif /app/usage_report.py csv_to_parquet -i ${f} -o ${f%.csv}.parquet

rm $f
