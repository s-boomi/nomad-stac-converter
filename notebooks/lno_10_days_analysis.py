import marimo

__generated_with = "0.15.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    from src.processing import RawDataAnalysis
    import matplotlib.pyplot as plt
    from mpl_toolkits.axes_grid1 import make_axes_locatable

    return RawDataAnalysis, make_axes_locatable, mo, pd, plt


@app.cell
def _(RawDataAnalysis):
    rda = RawDataAnalysis()
    df = rda.save_to_format("lno_10_days.geojson", "geojson")
    return (df,)


@app.cell
def _(df):
    sorted_df = df.sort_values(["utc_start_time", "diffraction_order"])
    sorted_df
    return


@app.cell
def _(df, make_axes_locatable, plt):
    fig, ax = plt.subplots(1, 1, figsize=(12, 6))

    ax.set_title("LNO on 10 days from NOMAD data")
    ax.set_xlabel("Lon")
    ax.set_ylabel("Lat")

    divider = make_axes_locatable(ax)
    cax = divider.append_axes("bottom", size="5%", pad=0.1)

    df.plot(
        column="diffraction_order",
        legend=True,
        ax=ax,
        cax=cax,
        legend_kwds={"label": "Diffraction order", "orientation": "horizontal"},
        cmap="copper",
    )
    return


@app.cell
def _(df, mo):
    # sliders
    datetime_picker = mo.ui.range_slider(
        start=df["utc_start_time"].min().timestamp(),
        stop=df["utc_end_time"].max().timestamp(),
        show_value=False,
        label="UTC dates",
        full_width=True,
    )
    return (datetime_picker,)


@app.cell
def _(df, mo):
    diffraction_order = mo.ui.dropdown.from_series(
        df["diffraction_order"].sort_values(),
        searchable=True,
        value=df["diffraction_order"].min().astype(str),
        full_width=True,
    )
    return (diffraction_order,)


@app.cell
def _(datetime_picker, diffraction_order, mo, pd):
    start_date, end_date = [
        pd.Timestamp.fromtimestamp(_v) for _v in datetime_picker.value
    ]

    date_fmt = f"""**Dates**

        *From:* {start_date}

        *To:* {end_date}
        """

    mo.vstack(
        [
            mo.hstack([datetime_picker, mo.md(date_fmt)], widths=[0.8, 0.2]),
            mo.hstack(
                [
                    diffraction_order,
                    mo.md(f"**Diffraction order:** {diffraction_order.value}"),
                ]
            ),
        ]
    )
    return end_date, start_date


@app.cell
def _(df, diffraction_order, end_date, start_date):
    time_mask = (df["utc_start_time"] >= start_date) & (df["utc_end_time"] <= end_date)

    filtered_df = df[(df["diffraction_order"] == diffraction_order.value) & time_mask]
    filtered_df
    return (filtered_df,)


@app.cell
def _(
    diffraction_order,
    end_date,
    filtered_df,
    make_axes_locatable,
    plt,
    start_date,
):
    filtered_fig, filtered_ax = plt.subplots(1, 1, figsize=(12, 6))

    filtered_ax.set_title(
        f"LNO from {start_date} to {end_date} on DFO {diffraction_order.value}"
    )
    filtered_ax.set_xlabel("Lon")
    filtered_ax.set_ylabel("Lat")

    # Set to lightgray for better contrast
    filtered_ax.set_facecolor("#222222")

    f_divider = make_axes_locatable(filtered_ax)
    f_cax = f_divider.append_axes("bottom", size="5%", pad=0.1)

    filtered_df.plot(
        column="channel_temperature",
        legend=True,
        ax=filtered_ax,
        cax=f_cax,
        legend_kwds={"label": "Channel Temperature", "orientation": "horizontal"},
        cmap="Wistia",
    )
    return


if __name__ == "__main__":
    app.run()
