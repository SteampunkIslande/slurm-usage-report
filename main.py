import polars as pl

from pathlib import Path
import sys


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
        .str.extract_groups(r"^(?<JobRoot>[^.])\.(?<_JobSuffix>.+)")
        .struct.unnest()
    )

    return (
        lf.with_columns(parts)
        .with_columns(
            pl.when(pl.col("_JobSuffix").is_null())
            .then(pl.lit("allocation"))
            .when(pl.col("_JobSuffix") == "batch")
            .then(pl.lit("batch"))
            .when(pl.col("_JobSuffix") == "extern")
            .then(pl.lit("extern"))
            # suffixe numérique → step srun
            .when(pl.col("_JobSuffix").str.contains(r"^\d+$"))
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
        .str.tail(1)
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
        (pl.col(colname) * 2**30).alias(f"{colname}_G" if keep_original else colname)
    )


def stats_job_aggregate_cols(
    lf: pl.LazyFrame,
    allocation_constant_cols: list[str],
    allocation_variable_cols: list[str],
) -> pl.LazyFrame:
    return lf.group_by("JobRoot").agg(
        [
            *[
                pl.col(col)
                .filter(pl.col("JobInfoType") != "allocation")
                .max()
                .alias(f"{col}_job")
                for col in allocation_variable_cols
            ],
            *[
                pl.col(col)
                .filter(pl.col("JobInfoType") == "allocation")
                .filter(pl.col(col).is_not_null())
                .first()
                .alias(f"{col}_alloc")
                for col in allocation_constant_cols
            ],
        ]
    )


def aggregate_per_alloc(lf: pl.LazyFrame) -> pl.LazyFrame:
    return stats_job_aggregate_cols(
        lf,
        ["ReqCPUS", "ReqMem", "ReqNodes"],
        [
            "Account",
            "AllocCPUS",
            "Comment",
            "CPUTime",
            "CPUTimeRAW",
            "DerivedExitCode",
            "Elapsed",
            "ElapsedRaw",
            "End",
            "ExitCode",
            "Group",
            "JobID",
            "JobName",
            "MaxDiskRead",
            "MaxDiskWrite",
            "MaxRSS",
            "MaxVMSize",
            "QOS",
            "Start",
            "State",
            "Submit",
            "SubmitLine",
            "Suspended",
            "SystemCPU",
            "Timelimit",
            "TimelimitRaw",
            "TotalCPU",
            "User",
            "UserCPU",
            "WorkDir",
        ],
    )


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
    lf = aggregate_per_alloc(lf)

    lf = add_units_kmg(lf, "MaxRSS")
    lf = convert_kmg_col(lf, "MaxRSS")
    lf = col_to_gigabytes(lf, "MaxRSS", keep_original=True)

    lf = add_units_kmg(lf, "ReqMem")
    lf = convert_kmg_col(lf, "ReqMem")
    lf = col_to_gigabytes(lf, "ReqMem", keep_original=True)

    lf = lf.with_columns(
        pl.col("MaxRSS").truediv(pl.col("ReqMem")).alias("MemEfficiencyRatio")
    )
    lf = lf.with_columns(
        pl.col("MemEfficiencyRatio").mul(100).alias("MemEfficiencyPercent")
    )


# A appeler depuis une fonction qui a pris un parquet en entrée, pour une seule exécution de snakemake
def snakemake_efficiency_report(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Un rapport spécifique à un pipeline snakemake.
    Répond à la question: pour chaque règle snakemake, quelles sont les moyennes, minimum, maximum, médianes de chaque métrique (déjà aggrégée ou non)
    """
    pass


# def generate_html_report(
#     sacct_parquet_files: list[Path],
#     output_html_report: Path,
# ):
#     """
#     A partir d'un ou plusieurs fichiers parquet, génère un rapport quotidien, hebdomadaire, mensuel ou annuel.
#     On y trouve un calendrier qui indique pour chaque journée le taux d'occupation réelle du cluster (somme des temps écoulé * MaxRSS)/(somme des temps écoulés * ReqMem)
#     """

#     lf = pl.scan_parquet(sacct_parquet_files)


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
