from deephaven_server import Server
s = Server(port=10000, jvm_args=["-Xmx4g",
                                       "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
                                       "-Dprocess.info.system-info.enabled=false"])
# for analysis
from deephaven import read_csv
import deephaven.agg as agg
import deephaven.updateby as uby

import deephaven.time as dhtu
import deephaven.pandas as dhpd

# other imports
import datetime as dt
import shinyswatch

def create_tables():

    stations = read_csv("../data/static/austin_bikeshare_stations.csv")
    trips = read_csv("../data/static/austin_bikeshare_trips.csv")

    # drop unwanted columns, filter rows, change types
    stations = stations.drop_columns("location")
    trips = trips.\
        drop_columns(["month", "year", "checkout_time", "end_station_name", "start_station_name"]).\
        update(["bikeid = (int)bikeid",
                "end_station_id = (int)end_station_id",
                "start_station_id = (int)start_station_id"]).\
        where(["!isNull(end_station_id)",
               "!isNull(start_station_id)"]).\
        update("start_time = parseInstant(start_time.replace(` `, `T`).concat(` CT`))")

    # aggregate subscriber_type into fewer categories and eliminate subscriber types with fewer than 100 subs
    trips = trips.\
        where("!isNull(subscriber_type)").\
        update(["subscriber_type = subscriber_type.contains(`24`) ? `24-Hour Vendor` : subscriber_type",
                "subscriber_type = subscriber_type.contains(`RideScout`) ? `24-Hour Vendor` : subscriber_type",
                "subscriber_type = subscriber_type.contains(`Republic`) ? `24-Hour Vendor` : subscriber_type",
                "subscriber_type = subscriber_type.contains(`Annual`) ? `Annual Membership` : subscriber_type",
                "subscriber_type = subscriber_type.contains(`Local365`) ? `Annual Membership` : subscriber_type",
                "subscriber_type = subscriber_type.contains(`Semester`) ? `Semester Membership` : subscriber_type",
                "subscriber_type = subscriber_type.contains(`Local30`) ? `30-Day Membership` : subscriber_type",
                "subscriber_type = subscriber_type.contains(`7-Day`) ? `7-Day Membership` : subscriber_type",
                "subscriber_type = subscriber_type.contains(`Weekender`) ? `Weekend Membership` : subscriber_type",
                "subscriber_type = subscriber_type.contains(`Explorer`) ? `1-Day Membership` : subscriber_type",
                "subscriber_type = subscriber_type.contains(`Founding`) ? `Founding Member` : subscriber_type",
                "subscriber_type = subscriber_type.contains(`Try Before`) ? `Trial Membership` : subscriber_type",
                "subscriber_type = subscriber_type.contains(`ACL`) ? `ACL` : subscriber_type"])

    trips = trips.\
        join(trips.count_by("sub_count", by = "subscriber_type"), on = "subscriber_type", joins = "sub_count").\
        where("sub_count >= 100").\
        drop_columns("sub_count")


    ### Frequency analysis

    # hourly and daily trip count
    hourly_ride_freq = trips.\
        update(["hour = hourOfDay(start_time, 'CT')",
                "hour = hour == 24 ? 23 : hour",
                "timestamp = toInstant(toLocalDate(start_time, 'CT'), millisOfDayToLocalTime((hour * 60 * 60 * 1000)), 'CT')"]).\
        count_by("trip_count", by = "timestamp").\
        sort("timestamp").\
        update(["day = dayOfYear(timestamp, 'CT')",
                "month = monthOfYear(timestamp, 'CT')",
                "year = year(timestamp, 'CT')"])

    hourly_ride_freq_by_day = hourly_ride_freq.\
        agg_by([agg.avg("avg_by_day = trip_count"),
                agg.std("std_by_day = trip_count")], by = ["day", "month", "year"])

    hourly_ride_freq_by_month = hourly_ride_freq.\
        agg_by([agg.avg("avg_by_month = trip_count"),
                agg.std("std_by_month = trip_count")], by = ["month", "year"])

    hourly_ride_freq_by_year = hourly_ride_freq.\
        agg_by([agg.avg("avg_by_year = trip_count"),
                agg.std("std_by_year = trip_count")], by = "year")

    hourly_ride_freq_stats = hourly_ride_freq_by_day.\
        join(hourly_ride_freq_by_month, on = ["month", "year"], joins = ["avg_by_month", "std_by_month"]).\
        join(hourly_ride_freq_by_year, on = "year", joins = ["avg_by_year", "std_by_year"])


    daily_ride_freq = hourly_ride_freq.\
        update("timestamp = atMidnight(timestamp, 'CT')").\
        agg_by([agg.sum_("trip_count"), agg.first(["day", "month", "year"])], by = "timestamp")

    daily_ride_freq_by_month = daily_ride_freq.\
        agg_by([agg.avg("avg_by_month = trip_count"),
                agg.std("std_by_month = trip_count")], by = ["month", "year"])

    daily_ride_freq_by_year = daily_ride_freq.\
        agg_by([agg.avg("avg_by_year = trip_count"),
                agg.std("std_by_year = trip_count")], by = "year")

    daily_ride_freq_stats = daily_ride_freq_by_month.\
        join(daily_ride_freq_by_year, on = "year", joins = ["avg_by_year", "std_by_year"])


    # hourly trip rolling average and standardization
    hourly_ride_freq_avg = hourly_ride_freq.\
        update_by(uby.rolling_avg_tick("trip_count_avg = trip_count", rev_ticks = 24, fwd_ticks = 24)).\
        join(hourly_ride_freq_stats.first_by(["month", "year"]), on = ["month", "year"], joins = ["avg_by_month", "std_by_month"]).\
        update("standardized_trip_count = (trip_count - avg_by_month) / std_by_month").\
        update_by(uby.rolling_avg_tick("standardized_trip_count_avg = standardized_trip_count", rev_ticks = 12, fwd_ticks = 12), by = ["month", "year"])

    # daily trip count rolling average and standardization
    daily_ride_freq_avg = daily_ride_freq.\
        update_by(uby.rolling_avg_tick("trip_count_avg = trip_count", rev_ticks = 15, fwd_ticks = 15)).\
        where(["year > 2013", "year < 2017"]).\
        join(daily_ride_freq_stats.first_by("year"), on = "year", joins = ["avg_by_year", "std_by_year"]).\
        update("standardized_trip_count = (trip_count - avg_by_year) / std_by_year").\
        update_by(uby.rolling_avg_tick("standardized_trip_count_avg = standardized_trip_count", rev_ticks = 15, fwd_ticks = 15), by = "year")


    ### Duration analysis

    hourly_ride_dur = trips.\
        update(["hour = hourOfDay(start_time, 'CT')",
                "hour = hour == 24 ? 23 : hour",
                "timestamp = toInstant(toLocalDate(start_time, 'CT'), millisOfDayToLocalTime((hour * 60 * 60 * 1000)), 'CT')"]).\
        agg_by([agg.sum_("duration_sum = duration_minutes"),
                agg.avg("duration_avg = duration_minutes"),
                agg.median("duration_med = duration_minutes")], by = "timestamp").\
        sort("timestamp").\
        update(["day = dayOfYear(timestamp, 'CT')",
                "month = monthOfYear(timestamp, 'CT')",
                "year = year(timestamp, 'CT')"])

    hourly_ride_dur_avg = hourly_ride_dur.\
        update_by(uby.rolling_avg_time("timestamp",
            ["duration_avg_sum = duration_sum",
             "duration_avg_avg = duration_avg",
             "duration_avg_med = duration_med"],
            rev_time = "PT24h", fwd_time = "PT24h"))

    daily_ride_dur = trips.\
        update(["timestamp = atMidnight(start_time, 'CT')"]).\
        agg_by([agg.sum_("duration_sum = duration_minutes"),
                agg.avg("duration_avg = duration_minutes"),
                agg.median("duration_med = duration_minutes")], by = "timestamp").\
        sort("timestamp").\
        update(["day = dayOfYear(timestamp, 'CT')",
                "month = monthOfYear(timestamp, 'CT')",
                "year = year(timestamp, 'CT')"])

    daily_ride_dur_avg = daily_ride_dur.\
        update_by(uby.rolling_avg_time("timestamp",
            ["duration_avg_sum = duration_sum",
             "duration_avg_avg = duration_avg",
             "duration_avg_med = duration_med"],
            rev_time = "P15D", fwd_time = "P15D"))

    return stations, trips, \
        hourly_ride_freq, hourly_ride_freq_avg, daily_ride_freq, daily_ride_freq_stats, daily_ride_freq_avg, \
        hourly_ride_dur, hourly_ride_dur_avg, daily_ride_dur, daily_ride_dur_avg


############################################################ APP #######################################################

from shiny import App, render, ui

app_ui = ui.page_fluid(
    shinyswatch.theme.yeti(),
    ui.panel_title("Austin Bikeshare Exploration"),

    ui.navset_tab(

        ui.nav("Raw Data",

            ui.row(
                ui.column(6,
                    "trips dataset",
                    ui.card(
                        ui.output_data_frame("trips_dataset")
                    )
                ),
                ui.column(6,
                    "stations dataset",
                    ui.card(
                        ui.output_data_frame("stations_dataset")
                    )
                )
            ),

            ui.row(
                "Input your own Deephaven query here!",
                ui.input_text("data_query", "This only supports single-expression queries.", width='100%'),
                ui.output_table("queried_dataset")
            )
        ),

        ui.nav("Through Time",

            ui.row(
                ui.column(6,
                    ui.navset_tab(
                        ui.nav("Daily ride count",
                            ui.row(
                                ui.input_date_range("freq_date_window", "Select a date range.",
                                                    start=None, end=None,
                                                    min=None, max=None)
                            )
                        ),
                        ui.nav("Hourly ride count",
                            ui.row(
                                ui.column(6,
                                ui.input_radio_buttons("freq_year", "Select a year.",
                                    choices=(2013, 2014, 2015, 2016, 2017), selected=2013, inline=True)
                                ),
                                ui.column(6,
                                    ui.input_selectize("freq_month", "Select a month.",
                                        choices=("January", "December"), selected="December", multiple=False, width=None)
                                )
                            )
                        ),
                        ui.nav("Daily ride duration",
                            ui.row(
                                ui.input_date_range("dur_date_window", "Select a date range.",
                                                    start=None, end=None,
                                                    min=None, max=None)
                            ),
                            ui.row(
                                ui.input_radio_buttons("daily_dur_stat", "Select a statistic of interest.",
                                                       choices=("Sum", "Average", "Median"), selected="Sum",
                                                       inline=True)
                            )
                        ),
                        ui.nav("Hourly ride duration",
                            ui.row(
                                ui.column(6,
                                ui.input_radio_buttons("dur_year", "Select a year.",
                                    choices=(2013, 2014, 2015, 2016, 2017), selected=2013, inline=True)
                                ),
                                ui.column(6,
                                    ui.input_selectize("dur_month", "Select a month.",
                                        choices=("January", "December"), selected="December", multiple=False, width=None)
                                )
                            ),
                            ui.row(
                                ui.input_radio_buttons("hourly_dur_stat", "Select a statistic of interest.",
                                                       choices=("Sum", "Average", "Median"), selected="Sum",
                                                       inline=True)
                            )
                        )
                    )
                ),

                ui.column(6,
                    "Some interesting research questions...",
                    ui.accordion(
                        ui.accordion_panel("How are subscription type and ride count related?"),
                        ui.accordion_panel("How does overall ride count trend compare from year to year?"),
                        ui.accordion_panel("Do the same months exhibit similar ride count trends from year to year?"),
                        ui.accordion_panel("Which months have the highest ride counts? Which have the lowest?"),
                        ui.accordion_panel("How does subscription status affect the overall distribution of ride duration?"),
                        ui.accordion_panel("What percentage of trips over 500 minutes are taken by different types of subscribers?"),
                        ui.accordion_panel("Do users tend to take longer rides on the weekends?"),
                        ui.accordion_panel("What is the overall intraday trend of ride duration for each day of the week?"),
                    id="research_questions"),
                )
            )
        ),

        ui.nav("Through Space",

            ui.row(
                ui.column(4,
                    ui.input_selectize("space_type", "Select a station type.",
                                       choices=("active", "closed", "moved", "ACL only"),
                                       selected=None, multiple=True),
                    ui.input_radio_buttons("space_size", "Size station points by:",
                                        choices = ("None", "Starting point popularity", "End point popularity"),
                                        selected="All", inline=False),
                    ui.input_radio_buttons("space_year", "Select a year.",
                                        choices=("All", 2013, 2014, 2015, 2016, 2017),
                                        selected="All", inline=True),
                    ui.input_selectize("space_month", "Select a month.",
                                        choices=("All", "January"),
                                        selected="All", multiple=False)
                ),
                ui.column(8, "map")
            )
        )
    )
)

def server(input, output, session):
    stations, trips, \
        hourly_ride_freq, hourly_ride_freq_avg, daily_ride_freq, daily_ride_freq_stats, daily_ride_freq_avg, \
        hourly_ride_dur, hourly_ride_dur_avg, daily_ride_dur, daily_ride_dur_avg = create_tables()

    trips_df = dhpd.to_pandas(trips)
    stations_df = dhpd.to_pandas(stations)

    @output
    @render.data_frame
    def trips_dataset():
        return render.DataGrid(
            trips_df,
            row_selection_mode="multiple",
            width=773,
            height=607,
        )

    @output
    @render.data_frame
    def stations_dataset():
        return render.DataGrid(
            stations_df,
            row_selection_mode="multiple",
            width=773,
            height=607,
        )

app = App(app_ui, server)
