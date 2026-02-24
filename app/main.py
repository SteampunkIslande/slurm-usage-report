#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
import polars as pl
import os
from pathlib import Path
import sys
import argparse, argcomplete
from snakemake_rules_plot import plot_snakemake_rule_efficicency


from utils import (
    INTERESTING_COLUMNS,
    USEFUL_COLUMNS,
    color_from_threshold_map,
    DEFAULT_CMAP,
)

import jinja2 as j2


# Première étape: rendre le fichier d'accounting sain
def sacct_sanitizer(
    input_filename: Path, output_filename: Path, col_count=109, separator="|"
) -> int:
    """Removes any line in `filename` with more than `col_count` columns, using `separator`.
    This means, `separator` has to be found exactly `col_count - 1` times in each line
    Returns the number of lines that were removed
    """
    lines_removed = 0
    with open(input_filename, "r") as fi, open(output_filename, "w") as fo:
        for line in fi:
            if line.count(separator) != col_count - 1:
                lines_removed += 1
                continue
            fo.write(line)
    return lines_removed


# Essentiel ! Ajoute JobRoot et JobInfoType (utile par la suite!)
def add_slurm_jobinfo_type_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Ajoute :
      - JobRoot: JobID parent (avant le '.')
      - JobInfoType: allocation | batch | extern | step | unknown

    Classification basée sur le suffixe du JobID:
      12345        -> allocation
      12345.batch  -> batch
      12345.extern -> extern
      12345.0      -> step
      12345.1      -> step
    """

    return (
        lf.with_columns(
            pl.col("JobID")
            .str.extract_groups(r"^(?<JobRoot>\d+)(?<_JobSuffix>\..+)?")
            .struct.unnest()
        )
        .with_columns(
            pl.when(pl.col("_JobSuffix").is_null())
            .then(pl.lit("allocation"))
            .when(pl.col("_JobSuffix") == ".batch")
            .then(pl.lit("batch"))
            .when(pl.col("_JobSuffix") == ".extern")
            .then(pl.lit("extern"))
            # suffixe numérique → step srun
            .when(pl.col("_JobSuffix").str.contains(r"^\.\d+$"))
            .then(pl.lit("step"))
            .otherwise(pl.lit("unknown"))
            .alias("JobInfoType"),
        )
        .drop("_JobSuffix")
    )


# Ajoute une colonne avec le même nom que colname, mais avec le suffixe `_unit`
def add_units_kmg(lf: pl.LazyFrame, colname: str) -> pl.LazyFrame:
    return lf.with_columns(
        pl.col(colname)
        .str.extract_groups(rf"(?<{colname}>\d+)(?<{colname}_unit>[KMGT])")
        .struct.unnest()
    )


# Convertit la colonne `colname` dans sa valeur équivalente en bytes (nécessite une colonne `{colname}_unit` existante, contenant K, M ou G en majuscules)
# Cette fonction supprime la colonne _unit associée (qui n'est plus vraiment nécessaire)
def convert_kmg_col(lf: pl.LazyFrame, colname: str) -> pl.LazyFrame:
    return lf.with_columns(
        pl.when(pl.col(colname).is_null())
        .then(None)
        .when(pl.col(f"{colname}_unit") == "K")
        .then(pl.col(colname).cast(pl.Int64) * 1024)
        .when(pl.col(f"{colname}_unit") == "M")
        .then(pl.col(colname).cast(pl.Int64) * 1024**2)
        .when(pl.col(f"{colname}_unit") == "G")
        .then(pl.col(colname).cast(pl.Int64) * 1024**3)
        .otherwise(None)
        .alias(colname)
    ).drop(f"{colname}_unit")


def col_to_gigabytes(
    lf: pl.LazyFrame, colname: str, keep_original=False
) -> pl.LazyFrame:
    return lf.with_columns(
        (pl.col(colname) / 2**30).alias(f"{colname}_G" if keep_original else colname)
    )


def aggregate_per_alloc(lf: pl.LazyFrame, group_col="JobRoot") -> pl.LazyFrame:

    aggregate_behavior = lambda col_name, col_type: {
        pl.Int64: pl.col(col_name).max().alias(col_name),
        pl.Float64: pl.col(col_name).max().alias(col_name),
        pl.String: pl.col(col_name).drop_nulls().first().alias(col_name),
    }[col_type]

    return lf.group_by(group_col).agg(
        [
            aggregate_behavior(col_name, col_type)
            for col_name, col_type in lf.collect_schema().items()
            if col_name != group_col
        ]
    )


def add_snakerule_col(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.with_columns(
        pl.col("Comment")
        .str.extract_groups(r"^rule_(?<rule_name>.+?)_wildcards_(?<wildcards>.+)")
        .struct.unnest()
    )


def aggregate_per_snakemake_rule(lf: pl.LazyFrame) -> pl.LazyFrame:

    lf = lf.group_by("rule_name").agg(
        # Rapport entre MaxRSS et ReqMem par règle
        pl.col("MemEfficiencyPercent").mean().name.suffix("_mean"),
        pl.col("MemEfficiencyPercent").median().name.suffix("_median"),
        pl.col("MemEfficiencyPercent").std().name.suffix("_std"),
        pl.col("MemEfficiencyPercent").min().name.suffix("_min"),
        pl.col("MemEfficiencyPercent").max().name.suffix("_max"),
        # Efficacité CPU par règle
        pl.col("CPUEfficiencyPercent").mean().name.suffix("_mean"),
        pl.col("CPUEfficiencyPercent").median().name.suffix("_median"),
        pl.col("CPUEfficiencyPercent").std().name.suffix("_std"),
        pl.col("CPUEfficiencyPercent").min().name.suffix("_min"),
        pl.col("CPUEfficiencyPercent").max().name.suffix("_max"),
        # Durée d'exécution par règle
        pl.col("ElapsedRaw").mean().name.suffix("_mean"),
        pl.col("ElapsedRaw").median().name.suffix("_median"),
        pl.col("ElapsedRaw").std().name.suffix("_std"),
        pl.col("ElapsedRaw").min().name.suffix("_min"),
        pl.col("ElapsedRaw").max().name.suffix("_max"),
        # Durée d'exécution minimale
        pl.col("Elapsed").min().alias("Elapsed_min"),
        # Durée d'exécution maximale
        pl.col("Elapsed").max().alias("Elapsed_max"),
        pl.col("QOS").drop_nulls().first(),
        pl.col("Account").drop_nulls().first(),
        pl.col("NodeList").drop_nulls().first(),
    )

    return lf


def parse_total_cpu_col(lf: pl.LazyFrame) -> pl.LazyFrame:
    # Calcul de l'efficacité CPU: TotalCPU / (ElapsedRaw * AllocCPUS) * 100
    # TotalCPU est le temps CPU utilisateur en secondes
    # Formats possibles: HH:MM:SS, MM:SS.ms, JJ-HH:MM:SS

    # Étape 1: Extraire les composants selon le format
    lf = lf.with_columns(
        pl.when(pl.col("TotalCPU").str.contains(r"^\d+-\d+:\d+:\d+$"))
        .then(
            pl.col("TotalCPU").str.extract_groups(
                r"(?<days>\d+)-(?<hours>\d+):(?<minutes>\d+):(?<seconds>\d+)"
            )
        )
        .when(pl.col("TotalCPU").str.contains(r"^\d+:\d+:\d+$"))
        .then(
            pl.col("TotalCPU").str.extract_groups(
                r"(?<hours>\d+):(?<minutes>\d+):(?<seconds>\d+)"
            )
        )
        .when(pl.col("TotalCPU").str.contains(r"^\d+:\d+\.\d+$"))
        .then(pl.col("TotalCPU").str.extract_groups(r"(?<minutes>\d+):(?<seconds>\d+)"))
        .otherwise(pl.lit(None))
        .alias("TotalCPU_parsed")
    )

    # Étape 2: Calculer les secondes à partir des composants
    lf = lf.with_columns(
        pl.when(pl.col("TotalCPU_parsed").struct.field("days").is_not_null())
        .then(
            pl.col("TotalCPU_parsed").struct.field("days").cast(pl.Int64) * 86400
            + pl.col("TotalCPU_parsed").struct.field("hours").cast(pl.Int64) * 3600
            + pl.col("TotalCPU_parsed").struct.field("minutes").cast(pl.Int64) * 60
            + pl.col("TotalCPU_parsed").struct.field("seconds").cast(pl.Int64)
        )
        .when(pl.col("TotalCPU_parsed").struct.field("hours").is_not_null())
        .then(
            pl.col("TotalCPU_parsed").struct.field("hours").cast(pl.Int64) * 3600
            + pl.col("TotalCPU_parsed").struct.field("minutes").cast(pl.Int64) * 60
            + pl.col("TotalCPU_parsed").struct.field("seconds").cast(pl.Int64)
        )
        .when(pl.col("TotalCPU_parsed").struct.field("minutes").is_not_null())
        .then(
            pl.col("TotalCPU_parsed").struct.field("minutes").cast(pl.Int64) * 60
            + pl.col("TotalCPU_parsed").struct.field("seconds").cast(pl.Int64)
        )
        .otherwise(pl.lit(None))
        .alias("TotalCPU_seconds")
    ).drop("TotalCPU_parsed")

    return lf


# A appeler depuis une fonction qui a pris un ou plusieurs parquets en entrée.
# Génère un lazyframe avec les colonnes les plus intéressantes pour avoir une idée générale de la consommation de mémoire notamment
def generic_report(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Un simple rapport qui, à partir d'un lazyframe, résume les ressources utilisées
    Répond à la question: pour chaque job, combien de pourcent de ce qui avait été demandé a réellement été utilisé
    """

    # Ajouter les colonnes JobRoot et JobInfoType (utile pour la suite)
    lf = add_slurm_jobinfo_type_columns(lf)
    # Aggrège les métriques

    lf = add_units_kmg(lf, "MaxRSS")
    lf = convert_kmg_col(lf, "MaxRSS")
    lf = col_to_gigabytes(lf, "MaxRSS", keep_original=True)

    lf = add_units_kmg(lf, "ReqMem")
    lf = convert_kmg_col(lf, "ReqMem")
    lf = col_to_gigabytes(lf, "ReqMem", keep_original=True)

    # Attention: tous les champs aggrégés le seront uniquement s'ils sont de type numérique
    lf = aggregate_per_alloc(lf)
    lf = lf.with_columns(
        pl.col("MaxRSS").truediv(pl.col("ReqMem")).alias("MemEfficiencyRatio")
    )
    lf = lf.with_columns(
        pl.col("MemEfficiencyRatio").mul(100).alias("MemEfficiencyPercent")
    )

    lf = parse_total_cpu_col(lf)

    lf = lf.with_columns(
        (
            pl.col("TotalCPU_seconds")
            .truediv(pl.col("ElapsedRaw") * pl.col("AllocCPUS"))
            .fill_nan(0)
            .mul(100)
        ).alias("CPUEfficiencyPercent")
    )

    return lf


def debug_parquet(input_parquet: Path, output_excel: Path):
    lf = pl.scan_parquet(input_parquet).select(INTERESTING_COLUMNS)
    lf.collect().write_excel(output_excel)


# Enregistre la sortie de SACCT (au format CSV) dans un format plus compact et plus rapide à requêter que CSV
def save_to_parquet(
    input_csv: Path,
    output_parquet: Path,
    verbose: bool = False,
    col_count: int = 109,
    separator: str = "|",
):
    sacct_file_overwrite = input_csv.with_suffix(".csv.tmp")
    removed_lines = sacct_sanitizer(
        input_csv, sacct_file_overwrite, col_count, separator
    )
    sacct_file_overwrite.replace(input_csv)

    if verbose:
        sys.stderr.write(
            f"{removed_lines} lignes ont été supprimées du fichier d'accounting"
        )

    pl.scan_csv(
        input_csv,
        separator="|",
        schema_overrides={
            "Account": pl.String,
            "AdminComment": pl.String,
            "AllocCPUS": pl.Int64,
            "AllocNodes": pl.Int64,
            "AllocTRES": pl.String,
            "AssocID": pl.Int64,
            "AveCPU": pl.String,
            "AveCPUFreq": pl.String,
            "AveDiskRead": pl.String,
            "AveDiskWrite": pl.String,
            "AvePages": pl.Int64,
            "AveRSS": pl.String,
            "AveVMSize": pl.String,
            "BlockID": pl.String,
            "Cluster": pl.String,
            "Comment": pl.String,
            "Constraints": pl.String,
            "ConsumedEnergy": pl.Int64,
            "ConsumedEnergyRaw": pl.Int64,
            "Container": pl.String,
            "CPUTime": pl.String,
            "CPUTimeRAW": pl.Int64,
            "DBIndex": pl.Int64,
            "DerivedExitCode": pl.String,
            "Elapsed": pl.String,
            "ElapsedRaw": pl.Int64,
            "Eligible": pl.String,
            "End": pl.String,
            "ExitCode": pl.String,
            "Flags": pl.String,
            "GID": pl.Int64,
            "Group": pl.String,
            "JobID": pl.String,
            "JobIDRaw": pl.String,
            "JobName": pl.String,
            "Layout": pl.String,
            "MaxDiskRead": pl.String,
            "MaxDiskReadNode": pl.String,
            "MaxDiskReadTask": pl.Int64,
            "MaxDiskWrite": pl.String,
            "MaxDiskWriteNode": pl.String,
            "MaxDiskWriteTask": pl.Int64,
            "MaxPages": pl.Int64,
            "MaxPagesNode": pl.String,
            "MaxPagesTask": pl.Int64,
            "MaxRSS": pl.String,
            "MaxRSSNode": pl.String,
            "MaxRSSTask": pl.Int64,
            "MaxVMSize": pl.String,
            "MaxVMSizeNode": pl.String,
            "MaxVMSizeTask": pl.Int64,
            "McsLabel": pl.String,
            "MinCPU": pl.String,
            "MinCPUNode": pl.String,
            "MinCPUTask": pl.Int64,
            "NCPUS": pl.Int64,
            "NNodes": pl.Int64,
            "NodeList": pl.String,
            "NTasks": pl.Int64,
            "Partition": pl.String,
            "Priority": pl.Int64,
            "QOS": pl.String,
            "QOSRAW": pl.Int64,
            "Reason": pl.String,
            "ReqCPUFreq": pl.String,
            "ReqCPUFreqGov": pl.String,
            "ReqCPUFreqMax": pl.String,
            "ReqCPUFreqMin": pl.String,
            "ReqCPUS": pl.Int64,
            "ReqMem": pl.String,
            "ReqNodes": pl.Int64,
            "ReqTRES": pl.String,
            "Reservation": pl.String,
            "ReservationId": pl.String,
            "Reserved": pl.String,
            "ResvCPU": pl.String,
            "ResvCPURAW": pl.Int64,
            "Start": pl.String,
            "State": pl.String,
            "Submit": pl.String,
            "SubmitLine": pl.String,
            "Suspended": pl.String,
            "SystemComment": pl.String,
            "SystemCPU": pl.String,
            "Timelimit": pl.String,
            "TimelimitRaw": pl.String,
            "TotalCPU": pl.String,
            "TRESUsageInAve": pl.String,
            "TRESUsageInMax": pl.String,
            "TRESUsageInMaxNode": pl.String,
            "TRESUsageInMaxTask": pl.String,
            "TRESUsageInMin": pl.String,
            "TRESUsageInMinNode": pl.String,
            "TRESUsageInMinTask": pl.String,
            "TRESUsageInTot": pl.String,
            "TRESUsageOutAve": pl.String,
            "TRESUsageOutMax": pl.String,
            "TRESUsageOutMaxNode": pl.String,
            "TRESUsageOutMaxTask": pl.String,
            "TRESUsageOutMin": pl.String,
            "TRESUsageOutMinNode": pl.String,
            "TRESUsageOutMinTask": pl.String,
            "TRESUsageOutTot": pl.String,
            "UID": pl.Int64,
            "User": pl.String,
            "UserCPU": pl.String,
            "WCKey": pl.String,
            "WCKeyID": pl.Int64,
            "WorkDir": pl.String,
        },
        quote_char=None,
    ).sink_parquet(output_parquet)


# Fonctions utilitaires CLI
def generic_usage_excel(input_parquet: Path, output_excel: Path):
    lf = pl.scan_parquet(input_parquet)
    lf = generic_report(lf)
    lf = lf.select(
        *[
            *USEFUL_COLUMNS,
            "ReqMem_G",
            "MaxRSS_G",
            "MemEfficiencyPercent",
            "CPUEfficiencyPercent",
        ]
    )
    lf.collect().write_excel(output_excel)


def generate_snakemake_efficiency_report(
    input_parquet: Path, output_html: Path, job_name: str = None
):
    lf = pl.scan_parquet(input_parquet)
    lf = generic_report(lf)
    lf = add_snakerule_col(lf)

    if job_name:
        lf = lf.filter(pl.col("JobName") == job_name)

    # Filtrer pour obtenir seulement les données avec des noms de règles
    lf = lf.filter(pl.col("rule_name").is_not_null())
    relaxed_df = lf.collect()

    mem_box_plot = plot_snakemake_rule_efficicency(relaxed_df, "MemEfficiencyPercent")
    cpu_box_plot = plot_snakemake_rule_efficicency(relaxed_df, "CPUEfficiencyPercent")
    runtime_box_plot = plot_snakemake_rule_efficicency(relaxed_df, "ElapsedRaw")

    lf = aggregate_per_snakemake_rule(lf)

    efficiency_table_mem = (
        lf.select(
            [
                "rule_name",
                "MemEfficiencyPercent_mean",
                "MemEfficiencyPercent_median",
                "MemEfficiencyPercent_std",
                "MemEfficiencyPercent_min",
                "MemEfficiencyPercent_max",
            ]
        )
        .collect()
        .sort("rule_name")
        .select(
            pl.col("rule_name").alias("Nom de la règle"),
            pl.col("MemEfficiencyPercent_mean").alias("Efficacité mémoire moyenne"),
            pl.col("MemEfficiencyPercent_median").alias("Efficacité mémoire médiane"),
            pl.col("MemEfficiencyPercent_std").alias("Efficacité mémoire (écart-type)"),
            pl.col("MemEfficiencyPercent_min").alias("Efficacité mémoire minimum"),
            pl.col("MemEfficiencyPercent_max").alias("Efficacité mémoire maximum"),
        )
        .to_dict(as_series=False)
    )
    efficiency_table_cpu = (
        lf.select(
            [
                "rule_name",
                "CPUEfficiencyPercent_mean",
                "CPUEfficiencyPercent_median",
                "CPUEfficiencyPercent_std",
                "CPUEfficiencyPercent_min",
                "CPUEfficiencyPercent_max",
            ]
        )
        .collect()
        .sort("rule_name")
        .select(
            pl.col("rule_name").alias("Nom de la règle"),
            pl.col("CPUEfficiencyPercent_mean").alias("Efficacité CPU moyenne"),
            pl.col("CPUEfficiencyPercent_median").alias("Efficacité CPU médiane"),
            pl.col("CPUEfficiencyPercent_std").alias("Efficacité CPU (écart-type)"),
            pl.col("CPUEfficiencyPercent_min").alias("Efficacité CPU minimum"),
            pl.col("CPUEfficiencyPercent_max").alias("Efficacité CPU maximum"),
        )
        .to_dict(as_series=False)
    )
    efficiency_table_runtime = (
        lf.select(
            [
                "rule_name",
                "ElapsedRaw_mean",
                "ElapsedRaw_median",
                "ElapsedRaw_std",
                "ElapsedRaw_min",
                "ElapsedRaw_max",
            ]
        )
        .collect()
        .sort("rule_name")
        .select(
            pl.col("rule_name").alias("Nom de la règle"),
            pl.col("ElapsedRaw_mean").alias("Durée moyenne"),
            pl.col("ElapsedRaw_median").alias("Durée médiane"),
            pl.col("ElapsedRaw_std").alias("Durée (écart-type)"),
            pl.col("ElapsedRaw_min").alias("Durée minimum"),
            pl.col("ElapsedRaw_max").alias("Durée maximum"),
        )
        .to_dict(as_series=False)
    )

    env = j2.Environment(
        loader=j2.FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates"))
    )
    env.filters["format_header"] = lambda t: t.replace("_", "\n")
    # Add filter that takes a list of (threshold, color) pairs and a value, and returns the color corresponding to the first threshold that the value is below
    env.filters["color_threshold"] = color_from_threshold_map
    template = env.get_template("snakemake_report_template.html.j2")
    output = template.render(
        {
            "mem_box_plot": mem_box_plot,
            "cpu_box_plot": cpu_box_plot,
            "runtime_box_plot": runtime_box_plot,
            "efficiency_table_mem": efficiency_table_mem,
            "efficiency_table_cpu": efficiency_table_cpu,
            "efficiency_table_runtime": efficiency_table_runtime,
            "color_config": {
                v: DEFAULT_CMAP
                for v in [
                    "Efficacité mémoire moyenne",
                    "Efficacité mémoire médiane",
                    "Efficacité mémoire minimum",
                    "Efficacité mémoire maximum",
                    "Efficacité CPU moyenne",
                    "Efficacité CPU médiane",
                    "Efficacité CPU minimum",
                    "Efficacité CPU maximum",
                ]
            },
        }
    )
    with open(output_html, "w") as f:
        f.write(output)


# CLI
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Outils de traitement des fichiers SLURM"
    )
    subparsers = parser.add_subparsers()

    # csv -> parquet
    p_csv = subparsers.add_parser(
        "csv_to_parquet", help="Convertir un fichier CSV en Parquet"
    )
    p_csv.set_defaults(func=save_to_parquet)
    p_csv.add_argument(
        "-i",
        "--input",
        dest="input_csv",
        type=Path,
        help="Chemin du fichier CSV d'entrée",
    )
    p_csv.add_argument(
        "-o",
        "--output",
        dest="output_parquet",
        type=Path,
        help="Chemin du fichier Parquet de sortie",
    )
    p_csv.add_argument(
        "--verbose", action="store_true", help="Afficher les informations détaillées"
    )
    p_csv.add_argument(
        "--col-count",
        type=int,
        default=109,
        help="Nombre attendu de colonnes dans le CSV (par défaut: 109)",
    )
    p_csv.add_argument(
        "--separator",
        type=str,
        default="|",
        help="Séparateur utilisé dans le CSV (par défaut: |)",
    )

    # generic efficiency report
    p_generic = subparsers.add_parser(
        "generic", help="Générer un Excel avec le taux d'utilisation de la RAM"
    )
    p_generic.set_defaults(func=generic_usage_excel)
    p_generic.add_argument(
        "--input",
        "-i",
        type=Path,
        help="Chemin du fichier Parquet d'entrée",
        required=True,
        dest="input_parquet",
    )
    p_generic.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Chemin du fichier Excel de sortie",
        required=True,
        dest="output_excel",
    )

    # Simple debug: parquet to excel
    p_debug = subparsers.add_parser(
        "debug", help="Générer un Excel directement à partir du parquet"
    )
    p_debug.set_defaults(func=debug_parquet)
    p_debug.add_argument(
        "-i",
        "--input",
        dest="input_parquet",
        type=Path,
        help="Chemin du fichier Parquet d'entrée",
    )
    p_debug.add_argument(
        "-o",
        "--output",
        dest="output_excel",
        type=Path,
        help="Chemin du fichier Excel de sortie",
    )

    # snakemake efficiency
    p_smk = subparsers.add_parser(
        "snakemake_efficiency",
        help="Générer un rapport HTML d'efficacité spécifique à Snakemake",
    )
    p_smk.set_defaults(func=generate_snakemake_efficiency_report)
    p_smk.add_argument(
        "--input",
        "-i",
        dest="input_parquet",
        type=Path,
        help="Chemin du fichier Parquet d'entrée",
    )
    p_smk.add_argument(
        "--output",
        "-o",
        dest="output_html",
        type=Path,
        help="Chemin du fichier html de sortie",
    )
    p_smk.add_argument(
        "--job-name", "-n", dest="job_name", help="Nom du job SLURM à sélectionner"
    )

    return parser


if __name__ == "__main__":
    parser = build_parser()
    argcomplete.autocomplete(parser)

    args = parser.parse_args()

    args = vars(args)
    func = args.pop("func")
    func(**args)
