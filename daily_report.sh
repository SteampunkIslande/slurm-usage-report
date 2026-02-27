#!/bin/bash

yesterday=$(date -d "yesterday" +%Y-%m-%d)

singularity exec -B /tmp /SINGULARITIES/slurm-usage-report-1.0.0.sif /app/daily_eff.py report --database /OPT/hermes/slurm-usage-report/SACCT_DB --date "$yesterday" --output "/OPT/hermes/slurm-usage-report/REPORTS/daily/$yesterday.html"