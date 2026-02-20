import matplotlib.pyplot as plt
import matplotlib.patches as patches
import polars as pl


def plot_cluster_memory_tiles(
    jobs: pl.DataFrame | pl.LazyFrame,
    total_mem_bytes: int,
    window_start=None,
    window_hours: float = 24,
    cmap="viridis",
):
    """
    Affiche une représentation 'cluster occupancy' mémoire.

    Rectangle global:
        largeur = fenêtre temporelle
        hauteur = mémoire totale cluster

    Chaque job:
        largeur = Elapsed
        hauteur = ReqMem
        couleur = MaxRSS / ReqMem

    Parameters
    ----------
    jobs : Polars DataFrame/LazyFrame avec colonnes:
        - Start (float): quand le job a démarré (0 si le job a démarré la veille par exemple)
        - Elapsed_seconds (float/int): le nombre de secondes durant lesquelles le job a tourné dans l'intervalle (une journée, de 00:00:00 à 23:59:59)
        - ReqMem_bytes (int): la quantité maximale de mémoire requise pour le job (en octets)
        - MaxRSS_bytes (int): la quantité maximale de mémoire utilisée pour le job (en octets)

    total_mem_bytes : mémoire totale cluster
    window_start : début fenêtre (sinon min(Start))
    window_hours : taille fenêtre (défaut 24h)
    cmap : colormap matplotlib
    """

    if isinstance(jobs, pl.LazyFrame):
        jobs = jobs.collect()

    # ---------- validation ----------
    required = {
        "Start",
        "Elapsed_seconds",
        "ReqMem_bytes",
        "MaxRSS_bytes",
    }
    missing = required - set(jobs.columns)
    if missing:
        raise ValueError(f"Colonnes manquantes: {missing}")

    # ---------- fenêtre temporelle ----------
    if window_start is None:
        window_start = jobs["Start"].min()

    window_end = window_start + pl.duration(hours=window_hours)

    # filtrer jobs dans fenêtre
    jobs = jobs.filter(
        (pl.col("Start") >= window_start) & (pl.col("Start") < window_end)
    )

    if jobs.is_empty():
        print("Aucun job dans la fenêtre.")
        return

    # ---------- ratio utilisation ----------
    jobs = jobs.with_columns(
        (pl.col("MaxRSS_bytes") / pl.col("ReqMem_bytes"))
        .clip(0, 1)
        .alias("usage_ratio")
    )

    # ---------- figure ----------
    fig, ax = plt.subplots(figsize=(12, 6))
    colormap = plt.get_cmap(cmap)
    norm = plt.Normalize(0, 1)

    # ---------- placement vertical (packing simple) ----------
    # on empile naïvement les jobs pour éviter les overlaps
    jobs = jobs.sort("Start")

    active = []  # [(end_time, y_top)]

    for row in jobs.iter_rows(named=True):
        start = row["Start"]
        elapsed = row["Elapsed_seconds"]
        reqmem = row["ReqMem_bytes"]
        ratio = row["usage_ratio"]

        x = (start - window_start).total_seconds() / 3600
        width = elapsed / 3600
        height = reqmem

        # libérer slots finis
        active = [(end, y) for end, y in active if end > start]

        # trouver position verticale libre
        y = 0
        while True:
            conflict = False
            for end, ytop in active:
                if not (y + height <= ytop - height or y >= ytop):
                    if y < ytop:
                        y = ytop
                        conflict = True
                        break
            if not conflict:
                break

        # dessiner rectangle
        rect = patches.Rectangle(
            (x, y),
            width,
            height,
            facecolor=colormap(norm(ratio)),
            edgecolor="black",
            linewidth=0.2,
        )
        ax.add_patch(rect)

        active.append((start + pl.duration(seconds=elapsed), y + height))

    # ---------- axes ----------
    ax.set_xlim(0, window_hours)
    ax.set_ylim(0, total_mem_bytes)

    ax.set_xlabel("Durée (heures)")
    ax.set_ylabel("Mémoire (bytes)")
    ax.set_title("Taux d'occupation de la mémoire du cluster (color = MaxRSS / ReqMem)")

    # ---------- colorbar ----------
    sm = plt.cm.ScalarMappable(norm=norm, cmap=colormap)
    sm.set_array([])
    plt.colorbar(sm, ax=ax, label="Ratio d'utilisation de la mémoire")

    plt.tight_layout()
    plt.show()
