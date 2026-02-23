#!/usr/bin/env python

# Give this script the path to a snakemake workflow directory.


import subprocess
import os
import re
from pathlib import Path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Post-run script for Snakemake to generate a Slurm usage report."
    )
    parser.add_argument(
        "workflow_dir",
        help="Path to the Snakemake workflow directory.",
    )
    args = parser.parse_args()

    workflow_dir = Path(args.workflow_dir).resolve()

    expected_log_dir = workflow_dir / ".snakemake" / "log"

    if not expected_log_dir.is_dir():
        print(f"Error: Expected log directory not found at {expected_log_dir}")
        exit(1)

    file_name_re = re.compile(
        r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2})(?P<minutes>\d{2})(?P<seconds>\d{2}).+\.snakemake\.log$"
    )

    for filename in expected_log_dir.iterdir():
        m = re.match(file_name_re, filename.name)
        if m:
            log_path = expected_log_dir / filename
            with log_path.open("r") as log_file:
                for line in log_file:
                    if "SLURM run ID: " in line:
                        job_id = line.strip()[14:]

                        sacct_report_path = Path(
                            f"sacct_report_{m.group('year')}-{m.group('month')}-{m.group('day')}T{m.group('hour')}:{m.group('minutes')}:{m.group('seconds')}.csv"
                        )
                        report_path_csv = workflow_dir / sacct_report_path
                        try:
                            with open(
                                workflow_dir / sacct_report_path,
                                "w",
                            ) as report_file:
                                # 1: Obtenir le rapport sacct pour le job_id
                                subprocess.run(
                                    [
                                        "sacct",
                                        "-S",
                                        f"{m.group('year')}-{m.group('month')}-{m.group('day')}",
                                        "-a",
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
                                # 2: convertir les CSV en parquet (plus rapide pour les requêtes)
                                subprocess.run(
                                    [
                                        "singularity",
                                        "exec",
                                        "/SINGULARITIES/slurm-usage-report-1.0.0.sif",
                                        "/app/main.py",
                                        "csv_to_parquet",
                                        "-i",
                                        workflow_dir / sacct_report_path,
                                        "-o",
                                        workflow_dir
                                        / sacct_report_path.with_suffix(".parquet"),
                                    ],
                                    text=True,
                                    check=True,
                                    env={**os.environ, "SINGULARITY_BIND": "/tmp"},
                                )
                                subprocess.run(
                                    [
                                        "singularity",
                                        "exec",
                                        "/SINGULARITIES/slurm-usage-report-1.0.0.sif",
                                        "/app/main.py",
                                        "snakemake_efficiency",
                                        "-i",
                                        workflow_dir
                                        / sacct_report_path.with_suffix(".parquet"),
                                        "-o",
                                        workflow_dir
                                        / sacct_report_path.with_suffix(".html"),
                                    ],
                                    text=True,
                                    check=True,
                                    env={**os.environ, "SINGULARITY_BIND": "/tmp"},
                                )

                                if report_path_csv.is_file():
                                    report_path_csv.unlink()

                        except subprocess.CalledProcessError as e:
                            print(f"Error calling sacct: {e}")
