from deephaven import ui
from deephaven.plot.figure import Figure

######################
#### RAW DATA TAB ####
######################

@ui.component
def create_query():
    query, set_query = ui.use_state("trips.update(\"month = monthOfYear(start_time, 'CT')\").where(\"month == 3\")")
    table = ui.use_memo(lambda : eval(query), [query])
    return ui.flex(
        "Input your own Deephaven query here!",
        ui.form(
            ui.text_field(value=query, name="query"),
            ui.button("Submit", type="submit"),
            on_submit=lambda data: set_query(data["query"])
        ),
        table,
        direction="column",
        flex_grow=1
    )

@ui.component
def raw_data_tab():
    return(
        ui.flex(
            ui.flex(
                ui.flex(
                    ui.text("Trips dataset"),
                    trips,
                    direction="column",
                    flex_grow=1
                ),
                ui.flex(
                    ui.text("Stations dataset"),
                    stations,
                    direction="column",
                    flex_grow=1
                ),
                direction="row",
                flex_grow=1
            ),
            ui.flex(
                create_query(),
                flex_grow=1,
                direction="column"
            ),
            direction="column",
            flex_grow=1,
            height="100%"
        )
    )

##########################
#### THROUGH TIME TAB ####
##########################

@ui.component
def daily_ride_count_tab():
    return ui.flex(
        ui.text("Select a date range."),
        daily_ride_freq_avg,
        direction="column",
        flex_grow=1
    )

@ui.component
def hourly_ride_count_tab():
    valid_years = [2013, 2014, 2015, 2016, 2017]
    selected_year, set_selected_year = ui.use_state(2014)

    return ui.flex(
        ui.flex(
            ui.flex(
                ui.text("Select a year."),
                ui.button_group(
                    *[ui.button(year, on_press = lambda year=year: set_selected_year(year)) for year in valid_years]
                ),
                direction="column"
            ),
            ui.text("Select a month."),
            direction="row",
            flex_grow=1
        ),
        hourly_ride_freq_avg.where(f"year == {selected_year}"),
        direction="column",
        flex_grow=1
    )

@ui.component
def daily_ride_duration_tab():
    valid_stats = ["Sum", "Average", "Median"]
    selected_stat, set_selected_stat = ui.use_state("Sum")

    if selected_stat == "Sum":
        daily_title, daily_stat_column, daily_rolling_stat_column = ("Daily sum of ride durations in minutes", "duration_sum", "duration_avg_sum")
    elif selected_stat == "Average":
        daily_title, daily_stat_column, daily_rolling_stat_column = ("Daily average ride duration in minutes", "duration_avg", "duration_avg_avg")
    elif selected_stat == "Median":
        daily_title, daily_stat_column, daily_rolling_stat_column = ("Daily median ride duration in minutes", "duration_med", "duration_avg_med")

    plot = Figure().\
        plot_xy(series_name=daily_title,
                t=daily_ride_dur_avg,
                x="timestamp",
                y=daily_stat_column).\
        plot_xy(series_name="30-day rolling average",
                t=daily_ride_dur_avg,
                x="timestamp",
                y=daily_rolling_stat_column).\
        show()
    
    return ui.flex(
        ui.text("Select a date range."),
        ui.text("Select a statistic of interest."),
        ui.button_group(
            *[ui.button(stat, on_press = lambda stat=stat: set_selected_stat(stat)) for stat in valid_stats]
        ),
        plot,
        direction="column",
        flex_grow=1
    )

@ui.component
def hourly_ride_duration_tab():
    valid_years = [2013, 2014, 2015, 2016, 2017]
    selected_year, set_selected_year = ui.use_state(2014)
    valid_stats = ["Sum", "Average", "Median"]
    selected_stat, set_selected_stat = ui.use_state("Sum")

    if selected_stat == "Sum":
        hourly_title, hourly_stat_column, hourly_rolling_stat_column = ("Hourly sum of ride durations in minutes", "duration_sum", "duration_avg_sum")
    elif selected_stat == "Average":
        hourly_title, hourly_stat_column, hourly_rolling_stat_column = ("Hourly average ride duration in minutes", "duration_avg", "duration_avg_avg")
    elif selected_stat == "Median":
        hourly_title, hourly_stat_column, hourly_rolling_stat_column = ("Hourly median ride duration in minutes", "duration_med", "duration_avg_med")

    plot = Figure().\
        plot_xy(series_name=hourly_title,
                t=hourly_ride_dur_avg.where(f"year == {selected_year}"),
                x="timestamp",
                y=hourly_stat_column).\
        plot_xy(series_name="30-day rolling average",
                t=hourly_ride_dur_avg.where(f"year == {selected_year}"),
                x="timestamp",
                y=hourly_rolling_stat_column).\
        show()

    return ui.flex(
        ui.flex(
            ui.flex(
                ui.text("Select a year."),
                ui.button_group(
                    *[ui.button(year, on_press = lambda year=year: set_selected_year(year)) for year in valid_years]
                ),
                direction="column",
            ),
            ui.text("Select a month."),
            direction="row",
            flex_grow=1
        ),
        ui.flex(
            ui.text("Select a statistic of interest."),
            ui.button_group(
                *[ui.button(stat, on_press = lambda stat=stat: set_selected_stat(stat)) for stat in valid_stats]
            ),
            direction="column",
            flex_grow=1
        ),
        plot,
        direction="column",
        flex_grow=1
    )

@ui.component
def f():
    print("hello!")
    return ui.panel(
        ui.text("hello"),
        "Question 1"
    )

@ui.component
def q1():
    _, open_panel = ui.use_state("Q1")
    return ui.action_button("Q1", on_press=lambda: open_panel(f()))

@ui.component
def through_time_tab():
    return ui.flex(
        ui.flex(
            ui.tabs(
                ui.tab_list(
                    ui.item("Daily ride count", key="Daily ride count"),
                    ui.item("Hourly ride count", key="Hourly ride count"),
                    ui.item("Daily ride duration", key="Daily ride duration"),
                    ui.item("Hourly ride duration", key="Hourly ride duration")
                ),
                ui.tab_panels(
                    ui.item(daily_ride_count_tab(), key="Daily ride count", flex_grow=1),
                    ui.item(hourly_ride_count_tab(), key="Hourly ride count"),
                    ui.item(daily_ride_duration_tab(), key="Daily ride duration"),
                    ui.item(hourly_ride_duration_tab(), key="Hourly ride duration")
                )
            ),
            flex_grow=1
        ),
        ui.flex(
            ui.text("Some interesting research questions..."),
            q1(),
            direction="column",
            flex_grow=1
        ),
        flex_grow=1
    )

###########################
#### THROUGH SPACE TAB ####
###########################

@ui.component
def selection_panel():
    return ui.flex(
        ui.text("Select a station type."),
        ui.text("Size station points by:"),
        ui.button_group(
            ui.button("None"),
            ui.button("Starting point popularity"),
            ui.button("Ending point popularity"),
            direction="column"
        ),
        ui.text("Select a year."),
        ui.button_group(
            ui.button("All"),
            ui.button(2013),
            ui.button(2014),
            ui.button(2015),
            ui.button(2016),
            ui.button(2017)
        ),
        ui.text("Select a month."),
        direction="column",
        flex_grow=1
    )

@ui.component
def map_panel():
    return ui.text("I'm a map")

@ui.component
def through_space_tab():
    return(
        ui.flex(
            selection_panel(),
            map_panel(),
            direction="row"
        )
    )

##########################
#### ALL TOGETHER NOW ####
##########################

@ui.component
def main_tabs():
    return [
      ui.panel(raw_data_tab(), title="Raw Data"),
      ui.panel(through_time_tab(), title="Through Time"),
      ui.panel(through_space_tab(), title="Through Space")
    ]

#bikeshare = main_tabs()