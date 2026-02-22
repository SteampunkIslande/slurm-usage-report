import matplotlib.pyplot as plt
import dayplot as dp
import polars as pl

import io
import base64


def plot_daily_efficiency(df: pl.DataFrame):

    fig, ax = plt.subplots(figsize=(15, 6))
    dp.calendar(
        dates=df["Dates"],
        values=df["MemoryEfficiency"],
        month_grid=True,
        ax=ax,
    )
    s = io.BytesIO()
    fig.savefig(s, format="png", bbox_inches="tight")
    s = base64.b64encode(s.getvalue()).decode("utf-8").replace("\n", "")
    return '<img src="data:image/png;base64,%s" />' % s
