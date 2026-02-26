# Rapports d'efficacité snakemake

- [x] Ajouter les métriques temps/input_size et memoire/input_size
- [x] Ajouter des explications sur le sens des métriques rapportées à input_size

# Rapport d'efficacité du cluster

- [x] Ajouter un script python (ou un point d'entrée dans la CLI de usage_report.py) qui permet de générer un calendrier de l'utilisation du cluster (calcule l'efficacité journalière du cluster)
- [x] Ajouter un script python qui génère un rapport quotidien de l'utilisation du cluster: taux d'occupation de la mémoire (en % de la capacité totale exprimée en GB*jour). Penser également à ajouter le temps d'attente min/max/médian/moyen dans le rapport quotidien.
- [x] Le rapport quotidien du cluster pourrait se trouver sous la forme intermédiaire d'un JSON, qui permettrait d'alimenter à la fois le calendrier et le rapport quotidien détaillé.
- [ ] Ajouter le nombre de job durant la journée (quel que soit leur état, qu'ils se soient terminé ce jour, qu'ils aient été soumis/démarrés ce jour, ou qu'ils aient tourné ce jour).