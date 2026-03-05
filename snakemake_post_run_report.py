#!/usr/bin/env python

import subprocess
from pathlib import Path
import argparse

if __name__ == "__main__":

    # Fichiers requis:
    # - Un fichier de log de snakemake (ex: .snakemake/log/xxx.log) pour extraire le SLURM job id
    # - Optionnellement, une base de données SACCT maison (dossier avec des fichiers parquet, les colonnes sont celles de sacct SLURM 22.05, soit 109 au total)
    # Fichiers produits:
    # - Un fichier parquet avec les données SACCT pour les jobs SLURM extraits
    # - Un fichier HTML avec le rapport d'efficacité de snakemake (temps CPU utilisé / temps CPU demandé) par règle snakemake, pour les jobs extraits
    # - Un fichier CSV avec la taille totale des fichiers d'entrée par job SLURM, pour les jobs extraits
    # Les trois fichiers produits seront nommés à partir du nom du fichier parquet intermédiaire,
    # celui donné avec l'argument -o, avec des suffixes différents (ex: xxx.parquet, xxx.html, xxx.input-sizes.csv)

    parser = argparse.ArgumentParser(
        description="Script post-run pour générer un rapport d'usage Slurm à partir des logs de Snakemake.\n"
        "Possibilité de spécifier plusieurs fichiers de log (ex: .snakemake/log/xxx.log) pour consolider les métriques\n"
        "à partir de plusieurs runs de Snakemake mais avec le même pipeline."
    )
    parser.add_argument(
        "-i",
        "--input",
        dest="log_paths",
        nargs="+",
        help="Path(s) to the Snakemake log(s) file(s) for which you'd like a Slurm usage report.",
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        help="Path to the output parquet file for the Slurm usage report.",
        required=True,
    )
    parser.add_argument(
        "-d",
        "--database",
        dest="database",
        help="Chemin vers la base de données SACCT maison du cluster (dossier avec les fichiers parquet).\n"
        "Permet de contourner sacct en cherchant directement les données dans les fichiers parquet",
    )
    parser.add_argument(
        "--singularity-image",
        help="Chemin absolu vers l'image singularity à utiliser pour exécuter l'application",
        default="/SINGULARITIES/slurm-usage-report-1.0.0.sif",
    )
    args = parser.parse_args()

    log_paths = [Path(p).resolve().absolute() for p in args.log_paths]
    output_parquet = Path(args.output).resolve().absolute()

    output_html = output_parquet.with_suffix(".html")
    output_input_sizes = output_parquet.with_suffix(".input-sizes.csv")

    singularity_image = args.singularity_image

    # 0: Obtenir le SLURM job id depuis le log de snakemake (il y a une ligne dédiée pour ça)
    slurm_job_names = subprocess.check_output(
        [
            "singularity",
            "exec",
            singularity_image,
            "/app/extract_snakemake_logs.py",
            "-i",
            *log_paths,
            "-o",
            output_parquet.with_suffix(
                ".input-sizes.csv"
            ),  # Permet ensuite de rapporter les métriques de run à la taille des fichiers
        ],
        text=True,
    ).splitlines()

    if not args.database:
        output_csv = output_parquet.with_suffix(".csv")
        output_raw_parquet = output_parquet.with_suffix(".raw.parquet")
        try:
            # Etapes 1 et 2: obtenir le rapport SACCT au format parquet
            # 1: Obtenir le rapport sacct pour le job_id
            with open(
                output_csv,
                "w",
            ) as report_file:
                subprocess.run(
                    [
                        "sacct",
                        "-S",
                        "1970-01-01",  # pour être sûr d'avoir tous les jobs, même ceux qui ont été lancés il y a longtemps
                        "-a",
                        "--name",
                        ",".join(slurm_job_names),
                        "-o",
                        "ALL",
                        "-P",
                    ],
                    stdout=report_file,
                    text=True,
                    check=True,
                )
            # 2: convertir les CSV en parquet (plus rapide pour les requêtes). Le schéma de celui-ci est dit 'raw' (vient directement de sacct)
            subprocess.run(
                [
                    "singularity",
                    "exec",
                    "-B",
                    "/tmp",
                    singularity_image,
                    "/app/usage_report.py",
                    "csv_to_parquet",
                    "-i",
                    output_csv,
                    "-o",
                    output_raw_parquet,
                ],
                text=True,
                check=True,
            )
            # Etape 3: A partir du fichier parquet obtenu aux étapes précédentes, obtenir le rapport d'efficacité snakemake
            subprocess.run(
                [
                    "singularity",
                    "exec",
                    "-B",
                    "/tmp",
                    singularity_image,
                    "/app/usage_report.py",
                    "snakemake_efficiency",
                    "-i",
                    output_raw_parquet,
                    "-o",
                    output_html,
                    "-s",
                    output_input_sizes,
                    "--output-parquet",
                    output_parquet,
                ],
                text=True,
                check=True,
            )
            if output_raw_parquet.exists():
                output_raw_parquet.unlink()
            if output_csv.exists():
                output_csv.unlink()

        except subprocess.CalledProcessError as e:
            print(f"Une erreur est survenue: {e}")
    else:
        database = Path(args.database)
        # Etape 3: A partir du fichier parquet obtenu aux étapes précédentes, obtenir le rapport d'efficacité snakemake
        subprocess.run(
            [
                "singularity",
                "exec",
                "-B",
                "/tmp",
                singularity_image,
                "/app/usage_report.py",
                "snakemake_efficiency",
                "-d",
                database,
                "-o",
                output_html,
                "-s",
                output_input_sizes,
                "--output-parquet",
                output_parquet,
            ],
            text=True,
            check=True,
        )
