#!/usr/bin/env python

# Give this script the path to a snakemake workflow directory.


import subprocess
import os
import re

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Post-run script for Snakemake to generate a Slurm usage report."
    )
    parser.add_argument(
        "workflow_dir", help="Path to the Snakemake workflow directory."
    )
    args = parser.parse_args()

    expected_log_dir = os.path.join(args.workflow_dir, ".snakemake", "log")

    if not os.path.isdir(expected_log_dir):
        print(f"Error: Expected log directory not found at {expected_log_dir}")
        exit(1)

    file_name_re = re.compile(
        r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2})(?P<minutes>\d{2})(?P<seconds>\d{2}).+\.snakemake\.log$"
    )

    for filename in os.listdir(expected_log_dir):
        m = re.match(file_name_re, filename)
        if m:
            log_path = os.path.join(expected_log_dir, filename)
            with open(log_path, "r") as log_file:

                for line in log_file:
                    if "SLURM run ID: " in line:
                        job_id = line.strip()[14:]

        sacct_report_path = f"sacct_report_{m.group('year')}-{m.group('month')}-{m.group('day')}T{m.group('hour')}:{m.group('minutes')}:{m.group('seconds')}.csv"
        try:
            with open(
                os.path.join(
                    args.workflow_dir,
                    sacct_report_path,
                ),
                "a",
            ) as report_file:
                # 1: Obtenir le rapport sacct pour le job_id
                subprocess.run(
                    [
                        "sacct",
                        "--name",
                        job_id,
                        "-o",
                        "ALL",
                        "-P",
                    ],
                    stdout=report_file,
                    text=True,
                    check=True,
                )
                # 2: convertir
                subprocess.run(
                    [
                        "singularity",
                        "exec",
                        "/SINGULARITIES/slurm-usage-report.sif",
                        "python",
                        "/app/main.py",
                        "csv_to_parquet",
                        "-i",
                        os.path.join(args.workflow_dir, sacct_report_path),
                        "-o",
                        os.path.join(
                            args.workflow_dir,
                            sacct_report_path.replace(".csv", ".parquet"),
                        ),
                    ],
                    text=True,
                    check=True,
                )
                subprocess.run(
                    [
                        "singularity",
                        "exec",
                        "/SINGULARITIES/slurm-usage-report.sif",
                        "python",
                        "/app/main.py",
                        "snakemake_efficiency",
                        "-i",
                        os.path.join(
                            args.workflow_dir,
                            sacct_report_path.replace(".csv", ".parquet"),
                        ),
                        "-o",
                        os.path.join(
                            args.workflow_dir,
                            sacct_report_path.replace(".csv", ".html"),
                        ),
                    ],
                    text=True,
                    check=True,
                )

        except subprocess.CalledProcessError as e:
            print(f"Error calling sacct: {e}")
