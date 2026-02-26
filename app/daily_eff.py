#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

"""
Calcule l'efficacité journalière du cluster

Métriques calculées:
- Taux d'occupation de la mémoire (pourcentage de la capacité en Gigaoctets*Jour)
- Taux d'occupation des CPUs (pourcentage de la capacité en Nombre de CPUs*Jour)
- Durée d'attente moyenne/médiane/minimale/maximale entre la soumission d'un job et le début de son exécution, pour chaque valeur de QOS

Usage:
    daily_eff.py report --date 2025-02-26 --db /path/to/database -o report.html
    daily_eff.py aggregate --from 2025-01-01 --to 2025-12-31 --db /path/to/database -o yearly.html
"""

import argparse
import argcomplete
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from plotjs import PlotJS

import jinja2 as j2
import polars as pl

from utils import add_daily_duration, add_wait_time_cols, add_job_duration_cols
from usage_report import generic_report


def compute_daily_metrics(
    lf: pl.LazyFrame, date: str, cluster_capacity: dict
) -> dict[str, Any]:
    """
    Calcule les métriques d'efficacité journalière à partir d'un LazyFrame.

    Args:
        lf: LazyFrame Polars contenant les données sacct
        date: Date cible au format 'YYYY-MM-DD'

    Returns:
        Dictionnaire contenant les métriques calculées
    """
    # Ajouter CPUTime_seconds
    lf = generic_report(lf)
    # Ajouter deux colonnes: la date demandée (format pl.Date, colonne `date`), et la durée de chaque job pendant cette date (colonne `daily_duration_hours`)
    lf = add_daily_duration(lf, date)

    # Ajoute les colonnes wait_time_seconds et wait_time_hours
    lf = add_wait_time_cols(lf)
    lf = add_job_duration_cols(lf)

    jobs_aggregations = [
        (pl.col("TotalCPU_seconds").sum()).alias("CPU.Secondes"),
        (pl.col("TotalCPU_seconds").sum() / 3600).alias("CPU.Heures"),
        (
            (
                (pl.col("TotalCPU_seconds")).sum()
                / pl.lit(cluster_capacity["cpu_secondes"])
            )
            * 100
        ).alias("Pourcentage d'utilisation CPU"),
        ((pl.col("MaxRSS") / 2**30) * pl.col("ElapsedRaw")).sum().alias("GB.Secondes"),
        (
            (
                ((pl.col("MaxRSS") / 2**30) * pl.col("ElapsedRaw")).sum()
                / pl.lit(cluster_capacity["gb_secondes"])
            )
            * 100
        ).alias("Taux d'occupation de la RAM"),
        pl.col("wait_time_seconds")
        .mean()
        .alias("Temps d'attente moyen en queue (secondes)"),
        pl.col("wait_time_seconds")
        .median()
        .alias("Temps d'attente médian en queue (secondes)"),
        pl.col("wait_time_seconds")
        .min()
        .alias("Temps d'attente minimum en queue (secondes)"),
        pl.col("wait_time_seconds")
        .max()
        .alias("Temps d'attente maximum en queue (secondes)"),
        pl.col("JobID")
        .filter(
            pl.col("Submit").str.to_datetime("%Y-%m-%dT%H:%M:%S").dt.date()
            == pl.lit(date).str.to_date()
        )
        .count()
        .alias("Jobs soumis"),
        pl.col("JobID")
        .filter(
            pl.col("Start").str.to_datetime("%Y-%m-%dT%H:%M:%S").dt.date()
            == pl.lit(date).str.to_date()
        )
        .count()
        .alias("Jobs démarrés"),
        pl.col("JobID")
        .filter(
            pl.col("Start").str.to_datetime("%Y-%m-%dT%H:%M:%S").dt.date()
            == pl.lit(date).str.to_date()
        )
        .count()
        .alias("Jobs terminés"),
        pl.col("JobID")
        .filter(pl.col("State") == "FAILED")
        .count()
        .alias("Jobs échoués"),
        pl.col("JobID")
        .filter(pl.col("State") == "COMPLETED")
        .count()
        .alias("Jobs terminés avec succès"),
        pl.col("JobID")
        .filter(pl.col("State") == "PREEMPTED")
        .count()
        .alias("Jobs préemptés"),
        pl.col("job_duration_seconds")
        .mean()
        .alias("Durée moyenne d'exécution (secondes)"),
        pl.col("job_duration_seconds")
        .median()
        .alias("Durée médiane d'exécution (secondes)"),
        pl.col("job_duration_seconds")
        .min()
        .alias("Durée minimum d'exécution (secondes)"),
        pl.col("job_duration_seconds")
        .max()
        .alias("Durée maximum d'exécution (secondes)"),
    ]

    job_aggregation_cols = [
        "CPU.Secondes",
        "CPU.Heures",
        "Pourcentage d'utilisation CPU",
        "GB.Secondes",
        "Taux d'occupation de la RAM",
        "Temps d'attente moyen en queue (secondes)",
        "Temps d'attente médian en queue (secondes)",
        "Temps d'attente minimum en queue (secondes)",
        "Temps d'attente maximum en queue (secondes)",
        "Jobs soumis",
        "Jobs démarrés",
        "Jobs terminés",
        "Jobs terminés avec succès",
        "Jobs échoués",
        "Jobs préemptés",
        "Durée moyenne d'exécution (secondes)",
        "Durée médiane d'exécution (secondes)",
        "Durée minimum d'exécution (secondes)",
        "Durée maximum d'exécution (secondes)",
    ]

    # Grouper par QOS
    lf_qos_grouped = (
        lf.group_by("QOS")
        .agg(*jobs_aggregations)
        .select(
            "QOS",
            *job_aggregation_cols,
        )
    )
    lf_global = (
        lf.group_by("date")
        .agg(*jobs_aggregations)
        .select("date", *job_aggregation_cols)
    )

    qos_metrics = lf_qos_grouped.collect().to_dicts()
    global_metrics = lf_global.collect().to_dicts()
    # Renvoie un dictionnaire de la forme: {"urgent":{"Temps moyen":15315,...}}
    return {
        **{
            row["QOS"]: {k: v for k, v in row.items() if k != "QOS"}
            for row in qos_metrics
        },
        **{
            "global": {k: v for k, v in row.items() if k != "date"}
            for row in global_metrics
        },
    }


def generate_daily_report(
    date: str, database: Path, output: Path, cluster_capacity: dict = None
):
    """
    Génère un rapport quotidien au format HTML et sauvegarde les métriques en JSON.

    Args:
        date: Date au format 'YYYY-MM-DD'
        database: Chemin vers la base de données (dossier avec fichiers parquet)
        output: Chemin du fichier HTML de sortie
    """
    # Charger les données
    expected_file = database / f"{date}.parquet"
    if not expected_file.is_file():
        print(f"Impossible de trouver {expected_file}", file=sys.stderr)
        sys.exit(1)

    lf = pl.scan_parquet(expected_file)

    # Calculer les métriques
    cluster_capacity = cluster_capacity or {
        "cpu_secondes": 96 * 86400 * 4 + 40 * 86400,
        "gb_secondes": 2183
        * 86400,  # Valeur hard codée à partir de la capacité totale de mémoire du cluster
    }

    metrics = compute_daily_metrics(lf, date, cluster_capacity)

    # Sauvegarder les métriques en JSON (même nom que le parquet)
    json_path = database / f"{date}.json"
    with open(json_path, "w") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)

    print(f"Métriques sauvegardées dans {json_path}", file=sys.stderr)

    # Générer le rapport HTML
    html_content = generate_daily_html_report(metrics, date)

    with open(output, "w") as f:
        f.write(html_content)

    print(f"Rapport HTML généré dans {output}", file=sys.stderr)


def format_duration(seconds: float) -> str:
    """Formate une durée en secondes en format lisible HH:MM:SS."""
    if seconds is None:
        return "N/A"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    elif minutes > 0:
        return f"{minutes}m {secs}s"
    else:
        return f"{secs}s"


def generate_daily_html_report(metrics: dict, date: str = None) -> str:
    """
    Génère le contenu HTML du rapport quotidien (sans JavaScript, envoyable par mail).

    Args:
        metrics: Dictionnaire des métriques calculées, de la forme:
            {
                "global": {"CPU.Heures": ..., "Taux d'occupation de la RAM": ..., ...},
                "qos_name": {"CPU.Heures": ..., "Taux d'occupation de la RAM": ..., ...}
            }
        date: Date du rapport au format 'YYYY-MM-DD'

    Returns:
        Contenu HTML du rapport
    """

    env = j2.Environment(
        loader=j2.FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates"))
    )
    env.filters["format_duration"] = format_duration
    template = env.get_template("daily_efficiency.html.j2")

    # Extraire les métriques globales
    global_metrics = metrics.get("global", {})

    # Créer un dictionnaire des métriques par QOS (sans "global")
    qos_metrics = {k: v for k, v in metrics.items() if k != "global"}

    html = template.render(
        date=date or "N/A",
        global_metrics=global_metrics,
        qos_metrics=qos_metrics,
        generation_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    return html


def generate_aggregate_report(
    from_date: str, to_date: str, database: Path, output: Path, no_js: bool = False
):
    """
    Génère un rapport agrégé avec calendriers pour une période donnée.

    Charge les fichiers JSON existants dans la base de données et génère
    un calendrier par métrique globale demandée.
    """
    import io
    import base64
    import matplotlib.pyplot as plt
    import dayplot as dp

    # Métriques à afficher (un calendrier par métrique)
    metrics_config = [
        {
            "key": "Taux d'occupation de la RAM",
            "title": "Taux d'occupation de la RAM (% de la capacité totale en GB.secondes)",
        },
        {
            "key": "Pourcentage d'utilisation CPU",
            "title": "Taux d'utilisation des CPUs (% de la capacité totale en CPU.secondes)",
        },
    ]

    # Charger les rapports JSON existants
    start = datetime.strptime(from_date, "%Y-%m-%d").date()
    end = datetime.strptime(to_date, "%Y-%m-%d").date()

    dates = []
    reports_data = []
    current = start

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        json_file = database / f"{date_str}.json"
        if json_file.exists():
            with open(json_file) as f:
                data = json.load(f)
                # On ne garde que les métriques globales
                if "global" in data:
                    dates.append(date_str)
                    reports_data.append(data["global"])
                else:
                    print(f"Rapport global manquant dans {json_file}", file=sys.stderr)
                    dates.append(date_str)
                    reports_data.append({})  # Valeurs vides pour les jours sans données
        else:
            print(f"Fichier JSON manquant: {json_file}", file=sys.stderr)
            dates.append(date_str)
            reports_data.append({})  # Valeurs vides pour les jours sans données
        current += timedelta(days=1)

    if not reports_data:
        print("Aucune donnée trouvée pour la période demandée.", file=sys.stderr)
        sys.exit(1)

    # Générer un calendrier pour chaque métrique
    calendars = []
    for metric_conf in metrics_config:
        metric_key = metric_conf["key"]
        metric_title = metric_conf["title"]

        # Extraire les valeurs pour cette métrique
        values = [r.get(metric_key, 0.0) for r in reports_data]

        # Créer le DataFrame pour le calendrier
        df_calendar = pl.DataFrame(
            {
                "Dates": dates,
                "Values": values,
            }
        ).with_columns(pl.col("Dates").str.to_date("%Y-%m-%d"))

        # Générer le calendrier avec matplotlib/dayplot
        fig, ax = plt.subplots(figsize=(15, 6))
        dp.calendar(
            dates=df_calendar["Dates"],
            values=df_calendar["Values"],
            month_grid=True,
            ax=ax,
        )

        if no_js:
            # Convertir en base64 (pour avoir une image statique, compatible avec les clients mail)
            s = io.BytesIO()
            fig.savefig(s, format="png", bbox_inches="tight")
            plt.close(fig)
            s.seek(0)
            img_base64 = base64.b64encode(s.getvalue()).decode("utf-8")
            calendar_html = f'<img src="data:image/png;base64,{img_base64}" />'
        else:
            PlotJS._favicon_path = ""
            PlotJS._document_title = ""
            calendar_html = (
                PlotJS(fig)
                .add_tooltip(
                    labels=df_calendar.with_columns(
                        pl.concat_str(
                            df_calendar["Values"].round(2).cast(pl.String), pl.lit(" %")
                        )
                    )["Values"],
                    hover_nearest=True,
                )
                .as_html()
            )

        calendars.append(
            {
                "title": metric_title,
                "calendar_html": calendar_html,
                "min_value": min(values) if values else 0,
                "max_value": max(values) if values else 100,
            }
        )

    # Charger et rendre le template Jinja2
    env = j2.Environment(
        loader=j2.FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates"))
    )
    template = env.get_template("aggregated_efficiency.html.j2")

    html = template.render(
        from_date=from_date,
        to_date=to_date,
        num_days=len(reports_data),
        calendars=calendars,
        generation_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    with open(output, "w") as f:
        f.write(html)

    print(f"Rapport agrégé généré dans {output}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    """Construit le parser d'arguments CLI."""
    parser = argparse.ArgumentParser(
        description="Calcul de l'efficacité journalière du cluster SLURM",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
    # Générer un rapport quotidien
    %(prog)s report --date 2025-02-26 --db /path/to/database -o report.html
    
    # Générer un rapport agrégé annuel
    %(prog)s aggregate --from 2025-01-01 --to 2025-12-31 --db /path/to/database -o yearly.html
""",
    )

    subparsers = parser.add_subparsers(required=True)

    # Sous-commande report
    p_report = subparsers.add_parser(
        "report", help="Générer un rapport HTML quotidien d'efficacité du cluster"
    )
    p_report.set_defaults(func=generate_daily_report)
    p_report.add_argument(
        "--date", "-d", required=True, help="Date du rapport au format YYYY-MM-DD"
    )
    p_report.add_argument(
        "--db",
        "--database",
        type=Path,
        required=True,
        dest="database",
        help="Chemin vers le dossier contenant les fichiers parquet bruts",
    )
    p_report.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        help="Chemin du fichier HTML de sortie",
    )

    # Sous-commande aggregate
    p_aggregate = subparsers.add_parser(
        "aggregate", help="Générer un rapport agrégé avec calendriers pour une période"
    )
    p_aggregate.set_defaults(func=generate_aggregate_report)
    p_aggregate.add_argument(
        "--from",
        required=True,
        dest="from_date",
        help="Date de début au format YYYY-MM-DD",
    )
    p_aggregate.add_argument(
        "--to", required=True, dest="to_date", help="Date de fin au format YYYY-MM-DD"
    )
    p_aggregate.add_argument(
        "--db",
        "--database",
        type=Path,
        required=True,
        dest="database",
        help="Chemin vers le dossier contenant les fichiers parquet/JSON",
    )
    p_aggregate.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        help="Chemin du fichier HTML de sortie",
    )
    p_aggregate.add_argument(
        "--no-js",
        action="store_true",
        help="Générer des calendriers statiques sans JavaScript (images PNG). Utile pour les clients mail ne supportant pas le JS.",
    )

    return parser


def main():
    parser = build_parser()
    argcomplete.autocomplete(parser)

    args = parser.parse_args()

    args = vars(args)
    func = args.pop("func")
    func(**args)


if __name__ == "__main__":
    main()
