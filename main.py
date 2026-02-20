import polars as pl

from pathlib import Path
import sys
import argparse

from collections.abc import Callable

from colnames import INTERESTING_COLUMNS


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

    # extraire les parties du JobID
    parts = (
        pl.col("JobID")
        .str.extract_groups(r"^(?<JobRoot>\d+)(?<_JobSuffix>\..+)?")
        .struct.unnest()
    )

    return (
        lf.with_columns(parts)
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
        .str.extract_groups(rf"(?i)(?<{colname}>\d+)(?<{colname}_unit>[kmgt])")
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


def smart_aggregate(
    lf: pl.LazyFrame,
    group_col: str,
    behavior: dict[str, Callable[[str], pl.Expr]] = None,
) -> pl.LazyFrame:
    """
    Agrège un LazyFrame en utilisant `group_col` comme clé de groupement.
    Pour chaque colonne, applique une fonction d'agrégation basée sur son type:
    - Int64 ou Float64: max
    - String: first non-null value
    Le paramètre `behavior` permet de spécifier une fonction d'agrégation personnalisée pour certaines colonnes,
    en fournissant un dictionnaire où les clés sont les noms de colonnes et les valeurs sont des fonctions
    prenant le nom de la colonne et retournant une expression d'agrégation Polars.

    Args:
        `lf`: le LazyFrame à agréger
        `group_col`: le nom de la colonne à utiliser pour le groupement
        `behavior`: un dictionnaire optionnel spécifiant des fonctions d'agrégation personnalisées pour certaines colonnes
    Returns:
        Un LazyFrame agrégé selon les règles définies ci-dessus.
    """

    default_behavior = lambda col_name, col_type: {
        pl.Int64: pl.col(col_name).max().alias(col_name),
        pl.Float64: pl.col(col_name).max().alias(col_name),
        pl.String: pl.col(col_name).drop_nulls().first().alias(col_name),
    }[col_type]

    aggregations = [
        (
            behavior[col_name](col_name)
            if behavior and col_name in behavior
            else default_behavior(col_name, col_type)
        )
        for col_name, col_type in lf.collect_schema().items()
        if col_name != group_col
    ]

    return lf.group_by(group_col).agg(aggregations)


def aggregate_per_alloc(lf: pl.LazyFrame, group_col="JobRoot") -> pl.LazyFrame:

    return smart_aggregate(
        lf,
        group_col,
    )


# A appeler depuis une fonction qui a pris un ou plusieurs parquets en entrée.
# Génère un lazyframe avec les colonnes les plus intéressantes pour avoir une idée générale de la consommation de mémoire notamment
def generic_report(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Un simple rapport qui, à partir d'un lazyframe, résume les ressources utilisées
    Répond à la question: pour chaque job, combien de pourcent de ce qui avait été demandé a réellement été utilisé
    """

    # Donne plus de sens aux cellules vides
    # lf = replace_empty_string_with_null(lf)
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

    return lf


# # A appeler depuis une fonction qui a pris un parquet en entrée, pour une seule exécution de snakemake
# def snakemake_efficiency_report(lf: pl.LazyFrame) -> pl.LazyFrame:
#     """
#     Un rapport spécifique à un pipeline snakemake.
#     Répond à la question: pour chaque règle snakemake, quelles sont les moyennes, minimum, maximum, médianes de chaque métrique (déjà aggrégée ou non)
#     """
#     # Implémentation simple : utiliser JobName_job comme clé de "règle" et fournir stats sur MemEfficiencyPercent
#     lf = add_slurm_jobinfo_type_columns(lf)
#     lf = aggregate_per_alloc(lf)
#     lf = add_units_kmg(lf, "MaxRSS")
#     lf = convert_kmg_col(lf, "MaxRSS")
#     lf = col_to_gigabytes(lf, "MaxRSS", keep_original=True)

#     lf = add_units_kmg(lf, "ReqMem")
#     lf = convert_kmg_col(lf, "ReqMem")
#     lf = col_to_gigabytes(lf, "ReqMem", keep_original=True)

#     lf = lf.with_columns(
#         pl.col("MaxRSS")
#         .truediv(pl.col("ReqMem"))
#         .mul(100)
#         .alias("MemEfficiencyPercent")
#     )

#     # Agréger par JobName_job (nom de job issu des lignes non-allocation)
#     # Si la colonne JobName_job n'existe pas, fallback vers JobName_alloc ou JobName
#     key = "JobName_job"
#     # construire aggregation
#     agg = lf.group_by(key).agg(
#         [
#             pl.col("MemEfficiencyPercent").mean().alias("MemEff_mean"),
#             pl.col("MemEfficiencyPercent").min().alias("MemEff_min"),
#             pl.col("MemEfficiencyPercent").max().alias("MemEff_max"),
#             pl.col("MemEfficiencyPercent").median().alias("MemEff_median"),
#             pl.count().alias("n_jobs"),
#         ]
#     )

#     return agg


def parquet_to_excel(parquet_in: Path, excel_out: Path):
    lf = (
        pl.scan_parquet(parquet_in)
        .select(INTERESTING_COLUMNS)
        .filter(pl.col("JobID").str.starts_with("31986"))
    )
    lf = generic_report(lf)
    lf.collect().write_excel(excel_out)


# Enregistre la sortie de SACCT (au format CSV) dans un format plus compact et plus rapide à requêter que CSV
def save_to_parquet(
    sacct_file: Path,
    parquet_out: Path,
    verbose: bool = False,
    col_count: int = 109,
    separator: str = "|",
):
    sacct_file_overwrite = sacct_file.with_suffix(".csv.tmp")
    removed_lines = sacct_sanitizer(
        sacct_file, sacct_file_overwrite, col_count, separator
    )
    sacct_file_overwrite.replace(sacct_file)

    if verbose:
        sys.stderr.write(
            f"{removed_lines} lignes ont été supprimées du fichier d'accounting"
        )

    pl.scan_csv(
        sacct_file,
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
    ).sink_parquet(parquet_out)


# Fonctions utilitaires CLI
def generate_ram_usage_excel(input_parquet: Path, output_excel: Path):
    lf = pl.scan_parquet(input_parquet)
    lf = generic_report(lf)
    lf.collect().write_excel(output_excel)


# def generate_snakemake_efficiency_excel(input_parquet: Path, output_excel: Path):
#     lf = pl.scan_parquet(input_parquet)
#     agg = snakemake_efficiency_report(lf)
#     df = agg.collect().to_pandas()
#     df.to_excel(output_excel, index=False)


# CLI
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Outils de traitement des fichiers SLURM"
    )
    subparsers = parser.add_subparsers(dest="command")

    # csv -> parquet
    p_csv = subparsers.add_parser(
        "csv_to_parquet", help="Convertir un fichier CSV en Parquet"
    )
    p_csv.add_argument("input_csv", type=Path, help="Chemin du fichier CSV d'entrée")
    p_csv.add_argument(
        "output_parquet", type=Path, help="Chemin du fichier Parquet de sortie"
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

    # ram usage report
    p_ram = subparsers.add_parser(
        "ram_usage", help="Générer un Excel avec le taux d'utilisation de la RAM"
    )
    p_ram.add_argument(
        "input_parquet", type=Path, help="Chemin du fichier Parquet d'entrée"
    )
    p_ram.add_argument(
        "output_excel", type=Path, help="Chemin du fichier Excel de sortie"
    )

    # Simple debug: parquet to excel
    p_debug = subparsers.add_parser(
        "debug", help="Générer un Excel directement à partir du parquet"
    )
    p_debug.add_argument(
        "input_parquet", type=Path, help="Chemin du fichier Parquet d'entrée"
    )
    p_debug.add_argument(
        "output_excel", type=Path, help="Chemin du fichier Excel de sortie"
    )

    # # snakemake efficiency
    # p_smk = subparsers.add_parser(
    #     "snakemake_efficiency",
    #     help="Générer un Excel d'efficacité spécifique à Snakemake",
    # )
    # p_smk.add_argument(
    #     "input_parquet", type=Path, help="Chemin du fichier Parquet d'entrée"
    # )
    # p_smk.add_argument(
    #     "output_excel", type=Path, help="Chemin du fichier Excel de sortie"
    # )

    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "csv_to_parquet":
        save_to_parquet(
            args.input_csv,
            args.output_parquet,
            verbose=args.verbose,
            col_count=args.col_count,
            separator=args.separator,
        )

    elif args.command == "ram_usage":
        generate_ram_usage_excel(args.input_parquet, args.output_excel)

    elif args.command == "debug":
        parquet_to_excel(args.input_parquet, args.output_excel)

    # elif args.command == "snakemake_efficiency":
    #     generate_snakemake_efficiency_excel(args.input_parquet, args.output_excel)

    else:
        parser.print_help()
