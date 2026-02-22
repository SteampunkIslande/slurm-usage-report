import polars as pl
import plotly.express as px


def plot_snakemake_rule_efficicency(df: pl.DataFrame, column: str):

    return px.box(df, x=column, y="rule_name", points="suspectedoutliers").to_html()
