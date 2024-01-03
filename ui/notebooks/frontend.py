from deephaven import ui
from deephaven.plot.figure import Figure

import deephaven.agg as agg
import deephaven.updateby as uby

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

    plot = Figure().\
        plot_xy(series_name="Daily ride count",
                t=daily_ride_freq_avg,
                x="timestamp",
                y="trip_count").\
        plot_xy(series_name="30-day rolling average",
                t=daily_ride_freq_avg,
                x="timestamp",
                y="trip_count_avg").\
        show()

    return ui.flex(
        ui.text("Select a date range."),
        plot,
        direction="column",
        flex_grow=1
    )

@ui.component
def hourly_ride_count_tab():
    valid_years = [2013, 2014, 2015, 2016, 2017]
    selected_year, set_selected_year = ui.use_state(2014)

    plot = Figure(). \
        plot_xy(series_name="Hourly ride count",
                t=hourly_ride_freq_avg.where(["year == (int)selected_year",
                                                "month == 3"]),
                x="timestamp",
                y="trip_count"). \
        plot_xy(series_name="24-hour rolling average",
                t=hourly_ride_freq_avg.where(["year == (int)selected_year",
                                                "month == 3"]),
                x="timestamp",
                y="trip_count_avg"). \
        show()

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
        plot,
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
def q1():
    plot = Figure(). \
        plot_pie(series_name="Ride Frequency by Subscription Type",
                    t=trips.count_by("count", by="subscriber_type"),
                    category="subscriber_type",
                    y="count"). \
        show()

    return ui.flex(
        ui.text("This plot shows how often users of each subscription types \
                go for a ride. Note that the various subscription types have \
                been aggregated into these 11 primary categories for simplicity."),
        plot,
        direction="column",
        flex_grow=1
    )

@ui.component
def q2():
    plot = Figure(). \
        plot_xy(series_name="Year-standardized trip count in 2014",
                t=daily_ride_freq_avg.where("year == 2014").update("day_of_year = dayOfYear(timestamp, 'CT')"),
                x="day_of_year",
                y="standardized_trip_count_avg"). \
        plot_xy(series_name="Year-standardized trip count in 2015",
                t=daily_ride_freq_avg.where("year == 2015").update("day_of_year = dayOfYear(timestamp, 'CT')"),
                x="day_of_year",
                y="standardized_trip_count_avg"). \
        plot_xy(series_name="Year-standardized trip count in 2016",
                t=daily_ride_freq_avg.where("year == 2016").update("day_of_year = dayOfYear(timestamp, 'CT')"),
                x="day_of_year",
                y="standardized_trip_count_avg"). \
        show()

    return ui.flex(
        ui.text("This plot shows trends from all three complete years. \
                The differences in magnitude and variation have been \
                removed to make the trend comparison as simple as possible."),
        plot,
        direction="column",
        flex_grow=1
    )

@ui.component
def q3():
    plot = Figure(rows=1, cols=3). \
        new_chart(row=0, col=0). \
        plot_xy(series_name="2014",
                t=hourly_ride_freq_avg.where(["year == 2014", "month == 3"]),
                x="timestamp",
                y="standardized_trip_count_avg"). \
        new_chart(row=0, col=1). \
        plot_xy(series_name="2015",
                t=hourly_ride_freq_avg.where(["year == 2015", "month == 3"]),
                x="timestamp",
                y="standardized_trip_count_avg"). \
        new_chart(row=0, col=2). \
        plot_xy(series_name="2016",
                t=hourly_ride_freq_avg.where(["year == 2016", "month == 3"]),
                x="timestamp",
                y="standardized_trip_count_avg"). \
        show()

    return ui.flex(
        ui.text("This plot shows trends from the selected month from all three \
                complete years. The differences in magnitude and variation have \
                been removed to make the trend comparison as simple as possible."),
        ui.text("Select a month."),
        plot,
        direction="column",
        flex_grow=1
    )

@ui.component
def q4():
    plot = Figure(). \
        plot_cat(series_name="2013",
                    t=daily_ride_freq.where("year == 2013").agg_by(agg.sum_("trip_count"), by="month"),
                    category="month", y="trip_count"). \
        plot_cat(series_name="2014",
                    t=daily_ride_freq.where("year == 2014").agg_by(agg.sum_("trip_count"), by="month"),
                    category="month", y="trip_count"). \
        plot_cat(series_name="2015",
                    t=daily_ride_freq.where("year == 2015").agg_by(agg.sum_("trip_count"), by="month"),
                    category="month", y="trip_count"). \
        plot_cat(series_name="2016",
                    t=daily_ride_freq.where("year == 2016").agg_by(agg.sum_("trip_count"), by="month"),
                    category="month", y="trip_count"). \
        plot_cat(series_name="2017",
                    t=daily_ride_freq.where("year == 2017").agg_by(agg.sum_("trip_count"), by="month"),
                    category="month", y="trip_count"). \
        show()

    return ui.flex(
        ui.text("This plot shows the ride count by month for a given year. If all \
                years are selected, only complete years will be included in the plot."),
        plot,
        direction="column",
        flex_grow=1
    )

@ui.component
def q5():
    plot = Figure(rows=2, cols=1). \
        new_chart(row=0, col=0). \
        plot_xy_hist(series_name="Annual subscribers",
                        t=trips.where("subscriber_type == `Annual Membership`"),
                        x="duration_minutes",
                        nbins=50,
                        xmin=0.0,
                        xmax=100.0). \
        new_chart(row=1, col=0). \
        plot_xy_hist(series_name="Non-annual subscribers",
                        t=trips.where("subscriber_type != `Annual Membership`"),
                        x="duration_minutes",
                        nbins=50,
                        xmin=0.0,
                        xmax=100.0). \
        show()

    return ui.flex(
        ui.text("This plot shows the distribution of ride duration for annual \
                subscribers and everyone else. Interestingly, annual subscribers \
                tend to take shorter trips."),
        plot,
        direction="column",
        flex_grow=1
    )

@ui.component
def q6():
    plot = Figure(). \
        plot_pie(series_name="Long Rides by Subscription Type",
                    t=trips.where("duration_minutes > 500").count_by("count", by="subscriber_type"),
                    category="subscriber_type",
                    y="count"). \
        show()

    return ui.flex(
        ui.text("This plot shows the percentage of trips over 500 minutes taken \
                by each subscription type. The majority of long trips were taken \
                by walk-up customers."),
        plot,
        direction="column",
        flex_grow=1
    )

@ui.component
def q7():
    day_of_week_name_array = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    q7_table = trips. \
        update(["day_of_week = dayOfWeek(start_time, 'CT')", ]). \
        agg_by([agg.avg("duration_avg = duration_minutes"),
                agg.median("duration_med = duration_minutes")], by="day_of_week"). \
        sort("day_of_week"). \
        update("day_of_week_name = (String)day_of_week_name_array[i]")

    plot = Figure(). \
        plot_cat(series_name="Average ride duration", t=q7_table, category="day_of_week_name",
                    y="duration_avg"). \
        plot_cat(series_name="Median ride duration", t=q7_table, category="day_of_week_name",
                    y="duration_med"). \
        show()

    return ui.flex(
        ui.text("This plot shows the average and median number of rides taken each \
                day of the week, aggregated over the entire dataset."),
        plot,
        direction="column",
        flex_grow=1
    )

@ui.component
def q8():
    q8_table = trips. \
        update(["day_of_week = dayOfWeek(start_time, 'CT')",
                "minute_of_day = minuteOfDay(start_time, 'CT')"]). \
        agg_by(agg.median("duration_minutes"), by=["minute_of_day", "day_of_week"]). \
        sort("minute_of_day")

    plot = Figure(). \
        plot_xy(series_name="Median intraday ride duration",
                t=q8_table.where("day_of_week == 4"),
                x="minute_of_day",
                y="duration_minutes"). \
        show()

    return ui.flex(
        ui.text("This plot shows the median minutely ride duration within each day, \
                aggregated over the entire dataset."),
        ui.text("Select a day."),
        plot,
        direction="column",
        flex_grow=1
    )

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

def selection_panel():
    valid_sizings = ["None", "Starting point popularity", "Ending point popularity"]
    selected_sizing, set_selected_sizing = ui.use_state("None")
    valid_years = ["All", 2013, 2014, 2015, 2016, 2017]
    selected_year, set_selected_year = ui.use_state("All")

    return selected_sizing, set_selected_sizing, \
        selected_year, set_selected_year, \
        ui.flex(
            ui.text("Select a station type."),
            ui.text("Size station points by:"),
            ui.button_group(
                *[ui.button(sizing, on_press = lambda sizing=sizing: set_selected_sizing(sizing)) for sizing in valid_sizings],
                direction="column"
            ),
            ui.text("Select a year."),
            ui.button_group(
                *[ui.button(year, on_press = lambda year=year: set_selected_year(year)) for year in valid_years]
            ),
            ui.text("Select a month."),
            direction="column",
            flex_grow=1
        )

@ui.component
def map_panel(sizing, set_sizing, year, set_year):
    print(sizing, set_sizing, year, set_year)
    return ui.text("I'm a map")



@ui.component
def through_space_tab():
    selected_sizing, set_selected_sizing, selected_year, set_selected_year, selections = selection_panel()
    return(
        ui.flex(
            selections,
            map_panel(selected_sizing, set_selected_sizing, selected_year, set_selected_year),
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