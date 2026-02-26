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

import jinja2 as j2
import polars as pl

from calendar_plot import plot_daily_efficiency
from utils import add_daily_duration, add_wait_time_cols
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
    # Ajouter deux colonnes: la date demandée (format pl.Date), et la durée de chaque job pendant cette date
    lf = add_daily_duration(lf, date)

    # Filtrer les jobs qui ont tourné ce jour (durée > 0)
    lf = lf.filter(pl.col("daily_duration_hours") > 0)
    lf = add_wait_time_cols(lf)

    # Grouper par QOS
    lf_qos_grouped = (
        lf.group_by("QOS")
        .agg(
            (pl.col("TotalCPU_seconds").sum()).alias("CPU.Secondes"),
            (pl.col("TotalCPU_seconds").sum() / 3600).alias("CPU.Heures"),
            (
                (
                    (pl.col("TotalCPU_seconds")).sum()
                    / pl.lit(cluster_capacity["cpu_secondes"])
                )
                * 100
            ).alias("Pourcentage d'utilisation CPU"),
            ((pl.col("MaxRSS") / 2**30) * pl.col("ElapsedRaw"))
            .sum()
            .alias("GB.Secondes"),
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
        )
        .select(
            "QOS",
            "CPU.Secondes",
            "CPU.Heures",
            "Pourcentage d'utilisation CPU",
            "GB.Secondes",
            "Taux d'occupation de la RAM",
            "Temps d'attente moyen en queue (secondes)",
            "Temps d'attente médian en queue (secondes)",
            "Temps d'attente minimum en queue (secondes)",
            "Temps d'attente maximum en queue (secondes)",
        )
    )
    lf_global = (
        lf.group_by("date")
        .agg(
            (pl.col("TotalCPU_seconds").sum()).alias("CPU.Secondes"),
            (pl.col("TotalCPU_seconds").sum() / 3600).alias("CPU.Heures"),
            (
                (
                    (pl.col("TotalCPU_seconds")).sum()
                    / pl.lit(cluster_capacity["cpu_secondes"])
                )
                * 100
            ).alias("Pourcentage d'utilisation CPU"),
            ((pl.col("MaxRSS") / 2**30) * pl.col("ElapsedRaw"))
            .sum()
            .alias("GB.Secondes"),
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
        )
        .select(
            "date",
            "CPU.Secondes",
            "CPU.Heures",
            "Pourcentage d'utilisation CPU",
            "GB.Secondes",
            "Taux d'occupation de la RAM",
            "Temps d'attente moyen en queue (secondes)",
            "Temps d'attente médian en queue (secondes)",
            "Temps d'attente minimum en queue (secondes)",
            "Temps d'attente maximum en queue (secondes)",
        )
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


def load_json_reports(database: Path, from_date: str, to_date: str) -> list[dict]:
    """
    Charge les rapports JSON entre deux dates.

    Args:
        database: Chemin vers le dossier contenant les fichiers JSON
        from_date: Date de début au format 'YYYY-MM-DD'
        to_date: Date de fin au format 'YYYY-MM-DD'

    Returns:
        Liste des métriques chargées depuis les fichiers JSON
    """
    start = datetime.strptime(from_date, "%Y-%m-%d").date()
    end = datetime.strptime(to_date, "%Y-%m-%d").date()

    reports = []
    current = start

    while current <= end:
        json_file = database / f"{current.strftime('%Y-%m-%d')}.json"
        if json_file.exists():
            with open(json_file) as f:
                reports.append(json.load(f))
        else:
            print(f"Il manque {json_file}", file=sys.stderr)
        current += timedelta(days=1)

    return reports


def generate_aggregate_report(
    from_date: str, to_date: str, database: Path, output: Path
):
    """
    Génère un rapport agrégé avec calendriers pour une période donnée.

    Args:
        from_date: Date de début au format 'YYYY-MM-DD'
        to_date: Date de fin au format 'YYYY-MM-DD'
        database: Chemin vers la base de données
        output: Chemin du fichier HTML de sortie
    """
    # Charger les rapports JSON existants
    reports = load_json_reports(database, from_date, to_date)

    # Créer un DataFrame pour les calendriers
    dates = [r["date"] for r in reports]
    cpu_hours = [r["total_cpu_hours"] for r in reports]
    mem_gb_hours = [r["total_memory_gb_hours"] for r in reports]
    job_counts = [r["job_count"] for r in reports]

    # Calculer l'efficacité mémoire (placeholder - à adapter selon vos besoins)
    # Pour l'instant, on utilise les GB*heures comme métrique
    df_calendar = pl.DataFrame(
        {
            "Dates": dates,
            "MemoryEfficiency": mem_gb_hours,
            "CPUHours": cpu_hours,
            "JobCount": job_counts,
        }
    )

    # Convertir les dates en format datetime
    df_calendar = df_calendar.with_columns(pl.col("Dates").str.to_date("%Y-%m-%d"))

    # Générer le calendrier
    calendar_html = plot_daily_efficiency(df_calendar)

    # Calculer les statistiques agrégées
    total_jobs = sum(job_counts)
    total_cpu_hours = sum(cpu_hours)
    total_mem_gb_hours = sum(mem_gb_hours)
    avg_jobs_per_day = total_jobs / len(reports) if reports else 0
    avg_cpu_hours_per_day = total_cpu_hours / len(reports) if reports else 0

    # Agréger les temps d'attente
    all_wait_times = []
    for r in reports:
        if r.get("wait_time_stats") and "mean_seconds" in r["wait_time_stats"]:
            all_wait_times.append(r["wait_time_stats"]["mean_seconds"])

    avg_wait_time = sum(all_wait_times) / len(all_wait_times) if all_wait_times else 0

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Rapport agrégé du cluster - {from_date} à {to_date}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        h1, h2 {{
            color: #333;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background-color: white;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .summary {{
            background-color: white;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .calendar {{
            text-align: center;
            margin: 20px 0;
        }}
        .calendar img {{
            max-width: 100%;
            height: auto;
        }}
    </style>
</head>
<body>
    <h1>Rapport agrégé d'efficacité du cluster</h1>
    <h2>Période: {from_date} à {to_date}</h2>

    <div class="summary">
        <h3>Statistiques globales de la période</h3>
        <table>
            <tr>
                <th>Métrique</th>
                <th>Valeur</th>
            </tr>
            <tr><td>Nombre de jours</td><td>{len(reports)}</td></tr>
            <tr><td>Nombre total de jobs</td><td>{total_jobs}</td></tr>
            <tr><td>Moyenne de jobs par jour</td><td>{avg_jobs_per_day:.1f}</td></tr>
            <tr><td>Total CPU*heures</td><td>{total_cpu_hours:.2f}</td></tr>
            <tr><td>Moyenne CPU*heures par jour</td><td>{avg_cpu_hours_per_day:.2f}</td></tr>
            <tr><td>Total Mémoire GB*heures</td><td>{total_mem_gb_hours:.2f}</td></tr>
            <tr><td>Temps d'attente moyen</td><td>{format_duration(avg_wait_time)}</td></tr>
        </table>
    </div>

    <div class="summary">
        <h3>Calendrier d'utilisation de la mémoire (GB*heures)</h3>
        <div class="calendar">
            {calendar_html}
        </div>
    </div>

    <div class="summary">
        <h3>Détail par jour</h3>
        <table>
            <tr>
                <th>Date</th>
                <th>Jobs</th>
                <th>CPU*heures</th>
                <th>Mémoire GB*heures</th>
            </tr>
            {"".join(f'<tr><td>{r["date"]}</td><td>{r["job_count"]}</td><td>{r["total_cpu_hours"]:.2f}</td><td>{r["total_memory_gb_hours"]:.2f}</td></tr>' for r in reports)}
        </table>
    </div>

    <footer>
        <p><em>Rapport généré automatiquement le {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em></p>
    </footer>
</body>
</html>
"""

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
