# slurm-usage-report

## Commandes récurrentes (exécution sur le cluster)

### `save_sacct.sh`

Le script `save_sacct.sh` doit être exécuté quotidiennement pour maintenir une base de données constituée d'une liste de fichiers parquet.
En l'occurrence, les fichiers sont sauvegardés dans un répertoire au même niveau que ce script, appelé `SACCT_DB` (emplacement hard codé).
Le chemin vers ce répertoire sert à tous les scripts d'analyse SLURM (rapport quotidien, rapport aggrégé, analyse de pipelines).

Un cron est en place pour exécuter ce script, tous les jours à 6h.

### `daily_report.sh`

Chaque jour, doit être généré le rapport d'efficacité du cluster. Pour cela, il est possible d'utiliser `daily_report.sh`, qui va éditer le rapport de la veille.
En l'occurrence, les fichiers sont sauvegardés dans un répertoire au même niveau que ce script, `REPORTS/daily/$(date -d 'yesterday' +%Y-%m-%d).html` (emplacement hard codé).
La génération de ce fichier va entraîner l'écriture (ou l'écrasement) d'un fichier nommé `SACCT_DB/$(date -d 'yesterday' +%Y-%m-%d).json` (emplacement hard codé).

Un cron est en place pour exécuter ce script, tous les jours à 6h.

## Analyse de run snakemake

Le fichier `snakemake_post_run_sacct.py` propose une CLI simple pour obtenir un fichier HTML de rapport de run à partir d'un ou plusieurs fichiers de log snakemake.

Les options:
- `-i`: Un ou plusieurs chemins (séparés par des espaces) vers les fichiers de log snakemake (ceux que l'on trouve dans .snakemake/log/DATE.log)
- `-d`: Chemin vers le dossier avec la base de données SACCT au format parquet
- `-o`: Chemin vers un fichier parquet non existant, qui pourra être exploité par l'administrateur système.
 