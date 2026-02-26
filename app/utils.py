import polars as pl
from datetime import datetime, timedelta

ALL_COLUMNS = [
    "Account",
    "AdminComment",
    "AllocCPUS",
    "AllocNodes",
    "AllocTRES",
    "AssocID",
    "AveCPU",
    "AveCPUFreq",
    "AveDiskRead",
    "AveDiskWrite",
    "AvePages",
    "AveRSS",
    "AveVMSize",
    "BlockID",
    "Cluster",
    "Comment",
    "Constraints",
    "ConsumedEnergy",
    "ConsumedEnergyRaw",
    "Container",
    "CPUTime",
    "CPUTimeRAW",
    "DBIndex",
    "DerivedExitCode",
    "Elapsed",
    "ElapsedRaw",
    "Eligible",
    "End",
    "ExitCode",
    "Flags",
    "GID",
    "Group",
    "JobID",
    "JobIDRaw",
    "JobName",
    "Layout",
    "MaxDiskRead",
    "MaxDiskReadNode",
    "MaxDiskReadTask",
    "MaxDiskWrite",
    "MaxDiskWriteNode",
    "MaxDiskWriteTask",
    "MaxPages",
    "MaxPagesNode",
    "MaxPagesTask",
    "MaxRSS",
    "MaxRSSNode",
    "MaxRSSTask",
    "MaxVMSize",
    "MaxVMSizeNode",
    "MaxVMSizeTask",
    "McsLabel",
    "MinCPU",
    "MinCPUNode",
    "MinCPUTask",
    "NCPUS",
    "NNodes",
    "NodeList",
    "NTasks",
    "Partition",
    "Priority",
    "QOS",
    "QOSRAW",
    "Reason",
    "ReqCPUFreq",
    "ReqCPUFreqGov",
    "ReqCPUFreqMax",
    "ReqCPUFreqMin",
    "ReqCPUS",
    "ReqMem",
    "ReqNodes",
    "ReqTRES",
    "Reservation",
    "ReservationId",
    "Reserved",
    "ResvCPU",
    "ResvCPURAW",
    "Start",
    "State",
    "Submit",
    "SubmitLine",
    "Suspended",
    "SystemComment",
    "SystemCPU",
    "Timelimit",
    "TimelimitRaw",
    "TotalCPU",
    "TRESUsageInAve",
    "TRESUsageInMax",
    "TRESUsageInMaxNode",
    "TRESUsageInMaxTask",
    "TRESUsageInMin",
    "TRESUsageInMinNode",
    "TRESUsageInMinTask",
    "TRESUsageInTot",
    "TRESUsageOutAve",
    "TRESUsageOutMax",
    "TRESUsageOutMaxNode",
    "TRESUsageOutMaxTask",
    "TRESUsageOutMin",
    "TRESUsageOutMinNode",
    "TRESUsageOutMinTask",
    "TRESUsageOutTot",
    "UID",
    "User",
    "UserCPU",
    "WCKey",
    "WCKeyID",
    "WorkDir",
]

USEFUL_COLUMNS = [
    "Account",
    "AllocCPUS",
    "AllocNodes",
    "AveCPU",
    "AveCPUFreq",
    "AveDiskRead",
    "AveDiskWrite",
    "AvePages",
    "AveRSS",
    "AveVMSize",
    "Comment",
    "CPUTime",
    "CPUTimeRAW",
    "DerivedExitCode",
    "Elapsed",
    "ElapsedRaw",
    "End",
    "ExitCode",
    "Flags",
    "Group",
    "JobID",
    "JobName",
    "MaxDiskRead",
    "MaxDiskReadNode",
    "MaxDiskReadTask",
    "MaxDiskWrite",
    "MaxDiskWriteNode",
    "MaxDiskWriteTask",
    "MaxPages",
    "MaxPagesNode",
    "MaxPagesTask",
    "MaxRSS",
    "MaxRSSNode",
    "MaxRSSTask",
    "MaxVMSize",
    "MaxVMSizeNode",
    "MaxVMSizeTask",
    "MinCPU",
    "MinCPUNode",
    "MinCPUTask",
    "NCPUS",
    "NNodes",
    "NodeList",
    "NTasks",
    "Partition",
    "QOS",
    "ReqCPUS",
    "ReqMem",
    "ReqNodes",
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
]

INTERESTING_COLUMNS = [
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
    "ReqCPUS",
    "ReqMem",
    "ReqNodes",
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
]


DEFAULT_CMAP = [
    ((0, 20), "#ff0000"),
    ((21, 60), "#d4a500"),
    ((61, 75), "#ffa500"),
    ((76, 100), "#008000"),
    ((101, 125), "#ffa500"),
    ((126, 150), "#ff0000"),
]

COLOR_MAPS = {"default": DEFAULT_CMAP}


def add_daily_duration(lazyframe: pl.LazyFrame, date: str) -> pl.LazyFrame:
    """
    Ajoute une colonne 'daily_duration_hours' qui contient la durée (en heures)
    pendant laquelle un job SLURM a tourné sur une journée spécifique.

    Args:
        lazyframe: LazyFrame Polars contenant les données sacct (avec les colonnes 'Start' et 'End')
        date: Date cible au format 'YYYY-MM-DD' (string ou objet date)

    Returns:
        LazyFrame avec une nouvelle colonne 'daily_duration_hours'

    Cas gérés:
        - Job started et terminé le même jour: durée complète (End - Start)
        - Job a démarré la veille, terminé ce jour: durée de minuit à End
        - Job a démarré ce jour, terminé le lendemain: durée de Start à minuit suivant
        - Job a spané plusieurs jours incluant cette date: 24 heures
        - Job n'a pas tourné ce jour: 0 heures
    """

    # Convertir la date en objet date si c'est une string
    if isinstance(date, str):
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
    else:
        target_date = date

    # Créer les limites de la journée cible
    day_start = datetime.combine(target_date, datetime.min.time())
    day_end = datetime.combine(target_date + timedelta(days=1), datetime.min.time())

    # Expressions Polars pour calculer la durée quotidienne
    # Convertir Start et End en datetime
    start_dt = pl.col("Start").str.to_datetime()
    end_dt = pl.col("End").str.to_datetime()

    # Créer les constantes pour les comparaisons
    day_start_dt = pl.lit(day_start)
    day_end_dt = pl.lit(day_end)

    # Calculer la durée selon les cas:
    # 1. Job started et terminé le même jour (jour ciblé)
    same_day = (start_dt.dt.date() == target_date) & (end_dt.dt.date() == target_date)
    duration_same_day = (end_dt - start_dt).dt.total_seconds() / 3600

    # 2. Job a démarré avant le jour ciblé, terminé ce jour
    started_before = start_dt < day_start_dt
    ended_on_day = end_dt.dt.date() == target_date
    case_started_before = started_before & ended_on_day
    duration_started_before = (end_dt - day_start_dt).dt.total_seconds() / 3600

    # 3. Job a démarré ce jour, terminé après
    started_on_day = start_dt.dt.date() == target_date
    ended_after = end_dt >= day_end_dt
    case_ended_after = started_on_day & ended_after
    duration_ended_after = (day_end_dt - start_dt).dt.total_seconds() / 3600

    # 4. Job a spané plusieurs jours (a commencé avant et terminé après)
    spanning = started_before & ended_after
    duration_spanning = pl.lit(24.0)

    # 5. Job n'a pas du tout tournée ce jour (hors période)
    # Cela sera traité comme défaut (0 heures)

    # Combiner les cas avec when/then/otherwise
    daily_duration = (
        pl.when(same_day)
        .then(duration_same_day)
        .when(case_started_before)
        .then(duration_started_before)
        .when(case_ended_after)
        .then(duration_ended_after)
        .when(spanning)
        .then(duration_spanning)
        .otherwise(0.0)
        .alias("daily_duration_hours")
    )

    return lazyframe.with_columns(daily_duration)
