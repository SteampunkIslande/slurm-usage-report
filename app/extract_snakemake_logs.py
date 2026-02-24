#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

"""Extraire la taille totale des fichiers d'entrée par SLURM job id depuis un fichier de log Snakemake.

Usage:
  python app/extract_snakemake_logs.py path/to/.snakemake/log/xxx.log [-o out.csv]

Sortie: CSV avec colonnes slurm_jobid,input_size_bytes
"""

from pathlib import Path
import argparse, argcomplete
import csv
import re
import sys
import os


RULE_RE = re.compile(r"^rule\s+(\S+):")
INPUT_RE = re.compile(r"^\s*input\s*:\s*(.+)", re.IGNORECASE)
SLURM_RE = re.compile(
    r"Job\s+(\d+)\s+has\s+been\s+submitted\s+with\s+SLURM\s+jobid\s+(\d+)\s+\(log:\s+(.+)\)\.$",
    re.IGNORECASE,
)


def find_project_dir(log_path: Path) -> Path | None:
    """Trouver le dossier d'exécution de snakemake en remontant jusqu'à '.snakemake'.
    Si non trouvé, retourne None.
    """
    if log_path.parent.name != "log" or log_path.parent.parent.name != ".snakemake":
        # Atypique, normalement, ce fichier de log est présent dans ${WORK_DIR}/.snakemake/log
        return None
    return log_path.parent.parent.parent.absolute().resolve()


def snakemake_log_records_generator(log_path: Path):
    """Générateur d'enregistrements : groupe les lignes entre deux lignes commençant par '['.

    Ignore le préambule avant la première ligne commençant par '['.
    """
    with log_path.open(encoding="utf-8", errors="ignore") as f:
        current_record = None
        for line in f:
            if line.startswith("["):
                # Début d'un nouvel enregistrement : si on avait un enregistrement en cours, on le rend
                if current_record is not None:
                    yield current_record
                current_record = [line]
            else:
                # Si on est dans un enregistrement, on ajoute la ligne, sinon on ignore le préambule
                if current_record is not None:
                    current_record.append(line)
        # Fin de fichier : rendre le dernier enregistrement s'il existe
        if current_record:
            yield current_record


def extract_from_record(record_lines):
    inputs = []
    slurm_id = ""
    job_id = ""
    rule_name = ""
    if not record_lines:
        return dict()
    for line in record_lines:
        m = INPUT_RE.match(line)
        if m:
            inputs = [p.strip() for p in m.group(1).strip().split(",")]
        s = SLURM_RE.match(line)
        if s:
            slurm_id = s.group(2)
            job_id = s.group(1)
        r = RULE_RE.match(line)
        if r:
            rule_name = r.group(1)
    return {
        "inputs": inputs,
        "slurm_id": slurm_id,
        "rule_name": rule_name,
        "job_id": job_id,
    }


def parse_log_file(log_path: Path, output_path: Path | None = None):
    pwd = os.getcwd()
    project_dir: Path = find_project_dir(log_path)
    if project_dir is not None:
        os.chdir(project_dir)

    records_gen = snakemake_log_records_generator(log_path)
    if output_path:
        out_file = output_path.open("w", newline="", encoding="utf-8")
    else:
        out_file = sys.stdout

    writer = csv.writer(out_file, delimiter="|")
    writer.writerow(
        ["slurm_jobid", "job_id", "rule_name", "input_size_bytes", "inputs"]
    )

    for record in records_gen:
        parsed_record = extract_from_record(record)
        # This record is of no interest (either no lines returned or no job_id found)
        if not parsed_record or not parsed_record["job_id"]:
            continue

        solved_inputs = [Path(p).absolute().resolve() for p in parsed_record["inputs"]]
        input_size_bytes = sum(p.stat().st_size for p in solved_inputs if p.exists())

        slurm_id = parsed_record["slurm_id"]
        rule_name = parsed_record["rule_name"]
        job_id = parsed_record["job_id"]
        writer.writerow(
            [
                slurm_id,
                job_id,
                rule_name,
                input_size_bytes,
                ",".join(str(p) for p in solved_inputs),
            ]
        )

    os.chdir(pwd)


def main():
    parser = argparse.ArgumentParser(
        description="Extraire tailles d'inputs par SLURM job id depuis un log Snakemake"
    )
    parser.add_argument(
        "-i",
        "--input",
        dest="logfile",
        help="Fichier de log Snakemake (ex: .snakemake/log/xxx.log)",
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        help="Fichier CSV de sortie (défaut: stdout)",
    )

    argcomplete.autocomplete(parser)

    args = parser.parse_args()

    log_path = Path(args.logfile)
    if not log_path.exists():
        print(f"Fichier de log introuvable: {log_path}", file=sys.stderr)
        sys.exit(2)

    output_path = Path(args.output) if args.output else None

    with log_path.open("r") as f:
        for line in f:
            if "SLURM run ID: " in line:
                # Petit bonus: tandis que les infos sur la taille des inputs sont extraites, on affiche aussi le SLURM run ID pour que
                # l'utilisateur puisse faire le lien entre les logs et les rapports sacct générés par snakemake_post_run_sacct.py
                print(line.strip()[14:])
                break

    parse_log_file(log_path, output_path)


if __name__ == "__main__":
    main()
