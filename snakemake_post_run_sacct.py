#!/usr/bin/env python

# Give this script the path to a snakemake workflow directory.


import subprocess
import os
import re
from pathlib import Path
import sys


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Post-run script for Snakemake to generate a Slurm usage report."
    )
    parser.add_argument(
        "log_path",
        help="Path to the Snakemake log file.",
    )
    args = parser.parse_args()

    log_path = Path(args.log_path).resolve()

    file_name_re = re.compile(
        r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2})(?P<minutes>\d{2})(?P<seconds>\d{2}).+\.snakemake\.log$"
    )
    m = file_name_re.match(log_path.name)
    if not m:
        print(
            f"Le nom du fichier de log ne correspond pas au format attendu: {log_path.name}",
            file=sys.stderr,
        )
        sys.exit(1)

    report_path_csv = log_path.with_suffix(".csv")
    # 0: Obtenir le SLURM job id depuis le log de snakemake (il y a une ligne dédiée pour ça)
    slurm_job_name = subprocess.check_output(
        [
            "singularity",
            "exec",
            "/SINGULARITIES/slurm-usage-report-1.0.0.sif",
            "/app/extract_snakemake_logs.py",
            "-i",
            log_path,
            "-o",
            report_path_csv.with_suffix(
                ".input-sizes.csv"
            ),  # servira pour une évolution de usage-report, pour rapporter les métriques d'efficacité en fonction de la taille des inputs
        ],
        text=True,
    ).rstrip()
    try:
        with open(
            report_path_csv,
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
                    slurm_job_name,
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
                "/app/usage-report.py",
                "csv_to_parquet",
                "-i",
                report_path_csv,
                "-o",
                report_path_csv.with_suffix(".parquet"),
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
                "/app/usage-report.py",
                "snakemake_efficiency",
                "-i",
                report_path_csv.with_suffix(".parquet"),
                "-o",
                report_path_csv.with_suffix(".html"),
            ],
            text=True,
            check=True,
            env={**os.environ, "SINGULARITY_BIND": "/tmp"},
        )

        if report_path_csv.is_file():
            report_path_csv.unlink()

    except subprocess.CalledProcessError as e:
        print(f"Error calling sacct: {e}")
