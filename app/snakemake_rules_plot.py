import polars as pl
import plotly.graph_objects as go


def plot_snakemake_rule_efficicency(df: pl.DataFrame, column: str, title: str):

    rule_names = sorted(df["rule_name"].unique().to_list(), reverse=True)

    # Créer les boutons pour le dropdown
    buttons = []

    # Bouton "ALL" pour afficher toutes les règles (par défaut)
    buttons.append(
        {
            "label": "ALL",
            "method": "restyle",
            "args": [{"visible": [True] * len(rule_names)}],
        }
    )

    # Boutons pour chaque règle individuelle
    for i, rule_name in enumerate(rule_names):
        visible = [False] * len(rule_names)
        visible[i] = True

        buttons.append(
            {"label": rule_name, "method": "restyle", "args": [{"visible": visible}]}
        )

    # Créer des traces séparées pour chaque règle
    fig = go.Figure()

    for rule_name in rule_names:
        rule_data = df.filter(pl.col("rule_name") == rule_name)

        fig.add_trace(
            go.Box(
                x=rule_data[column].to_list(),
                y=[rule_name] * len(rule_data),
                name=rule_name,
                boxpoints="suspectedoutliers",
                orientation="h",
            )
        )

    # Configurer le dropdown menu
    fig.update_layout(
        updatemenus=[
            {
                "buttons": buttons,
                "direction": "down",
                "showactive": True,
                "x": 1.0,
                "xanchor": "left",
                "y": 1.02,
                "yanchor": "top",
                "active": 0,  # Par défaut, sélectionne "ALL"
            }
        ],
        showlegend=False,
        xaxis_title=title,
        yaxis_title="Règles Snakemake",
        height=400
        + 30 * len(rule_names),  # Ajuster la hauteur en fonction du nombre de règles
    )

    return fig.to_html()
