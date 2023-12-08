### imports

# streamlit imports
import streamlit as st
from streamlit_deephaven import start_server, display_dh

### start the server
s = start_server(port=10000, jvm_args=["-Xmx4g",
                                       "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
                                       "-Dprocess.info.system-info.enabled=false"])

# for plotting
from deephaven.plot.figure import Figure

# for analysis
from deephaven import read_csv
import deephaven.agg as agg
import deephaven.updateby as uby

import deephaven.time as dhtu
import deephaven.pandas as dhpd

# other imports
import datetime as dt

############################################################ DATA #######################################################

st.set_page_config(layout="wide")

### read in and clean data
@st.cache_resource
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

stations, trips, \
    hourly_ride_freq, hourly_ride_freq_avg, daily_ride_freq, daily_ride_freq_stats, daily_ride_freq_avg, \
    hourly_ride_dur, hourly_ride_dur_avg, daily_ride_dur, daily_ride_dur_avg = create_tables()

MIN_DATE = dt.date(2013, 12, 22)
MAX_DATE = dt.date(2017, 8, 1)

available_months_df = dhpd.to_pandas(daily_ride_freq_stats)
AVAILABLE_MONTHS = {year: list(available_months_df.loc[available_months_df["year"] == year]["month"])
                    for year in (2013, 2014, 2015, 2016, 2017)}

MONTH_STR_TO_INT = {"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
                    "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12}

MONTH_INT_TO_STR = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
                    7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"}

WEEKDAY_STR_TO_INT = {"Monday": 1, "Tuesday": 2, "Wednesday": 3, "Thursday": 4, "Friday": 5, "Saturday": 6, "Sunday": 7}

st.subheader("Austin Bikeshare Exploration")
data_tab, time_tab, space_tab= st.tabs(["Raw Data", "Through Time", "Through Space"])

with data_tab:
    data_c1, data_c2 = st.columns(2)
    with data_c1:
        st.write("_trips_ dataset")
        display_dh(trips)
    with data_c2:
        st.write("_stations_ dataset")
        display_dh(stations)

    st.write("Input your own Deephaven query here!")
    st.text_input("This only supports single-expression queries.",
                  "trips.update(\"month = monthOfYear(start_time, 'CT')\").where(\"month == 3\")",
                  key = "data_query")
    custom_query_table = eval(st.session_state.data_query)
    display_dh(custom_query_table)

with time_tab:
    time_c1, time_c2 = st.columns(2)

    with time_c1:
        daily_count_tab, hourly_count_tab, daily_duration_tab, hourly_duration_tab = st.tabs(["Daily ride count", "Hourly ride count", "Daily ride duration", "Hourly ride duration"])

        with daily_count_tab:
            primary_frequency_plot_start, primary_frequency_plot_end = st.date_input(
                "Select a date range.",
                value = (MIN_DATE, MAX_DATE),
                min_value = MIN_DATE,
                max_value = MAX_DATE,
                key="freq_date_window"
            )
            primary_frequency_plot_start_j = dhtu.to_j_local_date(primary_frequency_plot_start)
            primary_frequency_plot_end_j = dhtu.to_j_local_date(primary_frequency_plot_end)
            daily_frequency_plot = Figure().\
                plot_xy(series_name="Daily ride count",
                        t=daily_ride_freq_avg.where(["toLocalDate(timestamp, 'CT') >= primary_frequency_plot_start_j",
                                                     "toLocalDate(timestamp, 'CT') <= primary_frequency_plot_end_j"]),
                        x="timestamp",
                        y="trip_count").\
                plot_xy(series_name="30-day rolling average",
                        t=daily_ride_freq_avg.where(["toLocalDate(timestamp, 'CT') >= primary_frequency_plot_start_j",
                                                     "toLocalDate(timestamp, 'CT') <= primary_frequency_plot_end_j"]),
                        x="timestamp",
                        y="trip_count_avg").\
                show()
            display_dh(daily_frequency_plot)

        with hourly_count_tab:
            hourly_count_tab_c1, hourly_count_tab_c2 = st.columns(2)
            with hourly_count_tab_c1:
                st.radio("Select a year.", (2013, 2014, 2015, 2016, 2017), key = "freq_year", horizontal = True)
            with hourly_count_tab_c2:
                st.selectbox("Select a month.", [MONTH_INT_TO_STR[month] for month in AVAILABLE_MONTHS[st.session_state.freq_year]], key = "freq_month")
            hourly_frequency_plot = Figure(). \
                plot_xy(series_name="Hourly ride count",
                        t=hourly_ride_freq_avg.where(["year == (int)st.session_state.freq_year",
                                                      "month == (int)MONTH_STR_TO_INT[st.session_state.freq_month]"]),
                        x="timestamp",
                        y="trip_count"). \
                plot_xy(series_name="24-hour rolling average",
                        t=hourly_ride_freq_avg.where(["year == (int)st.session_state.freq_year",
                                                      "month == (int)MONTH_STR_TO_INT[st.session_state.freq_month]"]),
                        x="timestamp",
                        y="trip_count_avg"). \
                show()
            display_dh(hourly_frequency_plot)

        with daily_duration_tab:
            primary_duration_plot_start, primary_duration_plot_end = st.date_input(
                "Select a date range.",
                value = (MIN_DATE, MAX_DATE),
                min_value = MIN_DATE,
                max_value = MAX_DATE,
                key = "dur_date_window"
            )
            st.radio("Select a statistic of interest.", ("Sum", "Average", "Median"), key="daily_dur_stat", horizontal=True)
            if st.session_state.daily_dur_stat == "Sum":
                daily_title, daily_stat_column, daily_rolling_stat_column = ("Daily sum of ride durations in minutes", "duration_sum", "duration_avg_sum")
            elif st.session_state.daily_dur_stat == "Average":
                daily_title, daily_stat_column, daily_rolling_stat_column = ("Daily average ride duration in minutes", "duration_avg", "duration_avg_avg")
            elif st.session_state.daily_dur_stat == "Median":
                daily_title, daily_stat_column, daily_rolling_stat_column = ("Daily median ride duration in minutes", "duration_med", "duration_avg_med")
            primary_duration_plot_start_j = dhtu.to_j_local_date(primary_duration_plot_start)
            primary_duration_plot_end_j = dhtu.to_j_local_date(primary_duration_plot_end)
            daily_duration_plot = Figure().\
                plot_xy(series_name=daily_title,
                        t=daily_ride_dur_avg.where(["toLocalDate(timestamp, 'CT') >= primary_duration_plot_start_j",
                                                     "toLocalDate(timestamp, 'CT') <= primary_duration_plot_end_j"]),
                        x="timestamp",
                        y=daily_stat_column).\
                plot_xy(series_name="30-day rolling average",
                        t=daily_ride_dur_avg.where(["toLocalDate(timestamp, 'CT') >= primary_duration_plot_start_j",
                                                     "toLocalDate(timestamp, 'CT') <= primary_duration_plot_end_j"]),
                        x="timestamp",
                        y=daily_rolling_stat_column).\
                show()
            display_dh(daily_duration_plot)

        with hourly_duration_tab:
            hourly_duration_c1, hourly_duration_c2 = st.columns(2)
            with hourly_duration_c1:
                st.radio("Select a year.", (2013, 2014, 2015, 2016, 2017), key = "dur_year", horizontal = True)
            with hourly_duration_c2:
                st.selectbox("Select a month.", [MONTH_INT_TO_STR[month] for month in AVAILABLE_MONTHS[st.session_state.dur_year]], key = "dur_month")
            st.radio("Select a statistic of interest.", ("Sum", "Average", "Median"), key="hourly_dur_stat", horizontal=True)
            if st.session_state.hourly_dur_stat == "Sum":
                hourly_title, hourly_stat_column, hourly_rolling_stat_column = ("Daily sum of ride durations in minutes", "duration_sum", "duration_avg_sum")
            elif st.session_state.hourly_dur_stat == "Average":
                hourly_title, hourly_stat_column, hourly_rolling_stat_column = ("Daily average ride duration in minutes", "duration_avg", "duration_avg_avg")
            elif st.session_state.hourly_dur_stat == "Median":
                hourly_title, hourly_stat_column, hourly_rolling_stat_column = ("Daily median ride duration in minutes", "duration_med", "duration_avg_med")
            hourly_duration_plot = Figure(). \
                plot_xy(series_name=hourly_title,
                        t=hourly_ride_dur_avg.where(["year == (int)st.session_state.dur_year",
                                                      "month == (int)MONTH_STR_TO_INT[st.session_state.dur_month]"]),
                        x="timestamp",
                        y=hourly_stat_column). \
                plot_xy(series_name="24-hour rolling average",
                        t=hourly_ride_dur_avg.where(["year == (int)st.session_state.dur_year",
                                                      "month == (int)MONTH_STR_TO_INT[st.session_state.dur_month]"]),
                        x="timestamp",
                        y=hourly_rolling_stat_column). \
                show()
            display_dh(hourly_duration_plot)

    with time_c2:
        st.write("Some interesting research questions...")

        with st.expander("How are subscription type and ride count related?"):
            st.write("This plot shows how often users of each subscription types go for a ride. Note that the various \
                     subscription types have been aggregated into these 11 primary categories for simplicity.")
            time_q1_plot = Figure(). \
                plot_pie(series_name="Ride Frequency by Subscription Type",
                         t=trips.count_by("count", by="subscriber_type"),
                         category="subscriber_type",
                         y="count"). \
                show()
            display_dh(time_q1_plot)

        with st.expander("How does overall ride count trend compare from year to year?"):
            st.write("This plot shows trends from all three complete years. The differences in magnitude \
                     and variation have been removed to make the trend comparison as simple as possible.")
            time_q2_plot = Figure(). \
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
            display_dh(time_q2_plot)

        with st.expander("Do the same months exhibit similar ride count trends from year to year?"):
            st.write("This plot shows trends from the selected month from all three complete years. The differences \
                     in magnitude and variation have been removed to make the trend comparison as simple as possible.")
            st.selectbox("Select a month.", (MONTH_STR_TO_INT.keys()), key = "freq_q3_month")
            time_q3_plot = Figure(rows=1, cols=3). \
                new_chart(row=0, col=0). \
                plot_xy(series_name="2014",
                        t=hourly_ride_freq_avg.where(["year == 2014", "month == (int)MONTH_STR_TO_INT[st.session_state.freq_q3_month]"]),
                        x="timestamp",
                        y="standardized_trip_count_avg"). \
                new_chart(row=0, col=1). \
                plot_xy(series_name="2015",
                        t=hourly_ride_freq_avg.where(["year == 2015", "month == (int)MONTH_STR_TO_INT[st.session_state.freq_q3_month]"]),
                        x="timestamp",
                        y="standardized_trip_count_avg"). \
                new_chart(row=0, col=2). \
                plot_xy(series_name="2016",
                        t=hourly_ride_freq_avg.where(["year == 2016", "month == (int)MONTH_STR_TO_INT[st.session_state.freq_q3_month]"]),
                        x="timestamp",
                        y="standardized_trip_count_avg"). \
                show()
            display_dh(time_q3_plot)

        with st.expander("Which months have the highest ride counts? Which have the lowest?"):
            st.write("This plot shows the ride count by month for a given year. If all years are selected, only complete \
                     years will be included in the plot.")
            time_q4_plot = Figure() \
                .plot_cat(series_name="2013",
                          t=daily_ride_freq.where("year == 2013").agg_by(agg.sum_("trip_count"), by="month"),
                          category="month", y="trip_count") \
                .plot_cat(series_name="2014",
                          t=daily_ride_freq.where("year == 2014").agg_by(agg.sum_("trip_count"), by="month"),
                          category="month", y="trip_count") \
                .plot_cat(series_name="2015",
                          t=daily_ride_freq.where("year == 2015").agg_by(agg.sum_("trip_count"), by="month"),
                          category="month", y="trip_count") \
                .plot_cat(series_name="2016",
                          t=daily_ride_freq.where("year == 2016").agg_by(agg.sum_("trip_count"), by="month"),
                          category="month", y="trip_count") \
                .plot_cat(series_name="2017",
                          t=daily_ride_freq.where("year == 2017").agg_by(agg.sum_("trip_count"), by="month"),
                          category="month", y="trip_count") \
                .show()
            display_dh(time_q4_plot)

        with st.expander("How does subscription status affect the overall distribution of ride duration?"):
            st.write("This plot shows the distribution of ride duration for annual subscribers and everyone else. \
                     Interestingly, annual subscribers tend to take shorter trips.")
            time_q5_plot = Figure(rows=2, cols=1). \
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
            display_dh(time_q5_plot)

        with st.expander("What percentage of trips over 500 minutes are taken by different types of subscribers?"):
            st.write("This plot shows the percentage of trips over 500 minutes taken by each subscription type. The \
                     majority of long trips are taken by walk-up customers.")
            time_q6_plot = Figure(). \
                plot_pie(series_name="Long Rides by Subscription Type",
                         t=trips.where("duration_minutes > 500").count_by("count", by="subscriber_type"),
                         category="subscriber_type",
                         y="count"). \
                show()
            display_dh(time_q6_plot)

        with st.expander("Do users tend to take longer rides on the weekends?"):
            st.write("This plot shows the average and median number of rides taken each day of the week, aggregated \
                     over the entire dataset. ")
            day_of_week_name_array = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            time_q7_table = trips. \
                update(["day_of_week = dayOfWeek(start_time, 'CT')", ]). \
                agg_by([agg.avg("duration_avg = duration_minutes"),
                        agg.median("duration_med = duration_minutes")], by="day_of_week"). \
                sort("day_of_week"). \
                update("day_of_week_name = (String)day_of_week_name_array[i]")

            time_q7_plot = Figure(). \
                plot_cat(series_name="Average ride duration", t=time_q7_table, category="day_of_week_name",
                         y="duration_avg"). \
                plot_cat(series_name="Median ride duration", t=time_q7_table, category="day_of_week_name",
                         y="duration_med"). \
                show()
            display_dh(time_q7_plot)

        with st.expander("What is the overall intraday trend of ride duration for each day of the week?"):
            st.write("This plot shows the median minutely ride duration within each day, aggregated over the entire \
                     dataset.")
            st.selectbox("Select a day.", (WEEKDAY_STR_TO_INT.keys()), key = "time_q8_day")
            time_q8_table = trips. \
                update(["day_of_week = dayOfWeek(start_time, 'CT')",
                        "minute_of_day = minuteOfDay(start_time, 'CT')"]). \
                agg_by(agg.median("duration_minutes"), by=["minute_of_day", "day_of_week"]). \
                sort("minute_of_day")

            time_q8_plot = Figure(). \
                plot_xy(series_name="Median intraday ride duration",
                        t=time_q8_table.where("day_of_week == (int)WEEKDAY_STR_TO_INT[st.session_state.time_q8_day]"),
                        x="minute_of_day",
                        y="duration_minutes"). \
                show()
            display_dh(time_q8_plot)

with space_tab:
    space_c1, space_c2 = st.columns((0.3, 0.7))
    stations_color = stations.update("color = status == `active` ? `#1EB025` : status == `closed` ? `#DF0B0B` : status == `moved` ? `#CDC70E` : `#1859EE`")

    with space_c1:
        st.multiselect("Select a station type.", ("active", "closed", "moved", "ACL only"),
                       default = ("active", "closed", "moved", "ACL only"), key = "space_type")
        st.radio("Size station points by:", ("None", "Starting point popularity", "End point popularity"), key = "space_size")
        st.radio("Select a year.", ("All", 2013, 2014, 2015, 2016, 2017), key = "space_year", horizontal = True)
        selectable_months = ["All", *[MONTH_INT_TO_STR[month] for month in AVAILABLE_MONTHS[st.session_state.space_year]]] \
            if st.session_state.space_year != "All" else ["All", *MONTH_STR_TO_INT.keys()]
        st.selectbox("Select a month.", selectable_months, key = "space_month")

        if len(st.session_state.space_type) == 4:
            space_table = stations_color
        else:
            space_table = stations_color.where_one_of(["status == `" + station_type + "`" for station_type in st.session_state.space_type])
        if st.session_state.space_size != "None":
            filtered_trips = trips
            filter_query = []
            if st.session_state.space_year != "All":
                filter_query.append("year == " + str(st.session_state.space_year))
            if st.session_state.space_month != "All":
                filter_query.append("month == " + str(MONTH_STR_TO_INT[st.session_state.space_month]))
            if filter_query:
                filtered_trips = filtered_trips.\
                    update(["year = year(start_time, 'CT')", "month = monthOfYear(start_time, 'CT')"]).\
                    where(filter_query)
            if st.session_state.space_size == "Starting point popularity":
                space_table = space_table.join(filtered_trips.count_by("count", by = "start_station_id"), on = "station_id = start_station_id")
            else:
                space_table = space_table.join(filtered_trips.count_by("count", by="end_station_id"), on="station_id = end_station_id")

    with space_c2:
        space_table_df = dhpd.to_pandas(space_table)
        if st.session_state.space_size != "None":
            space_table_df["count"] = 2000 * space_table_df["count"] / space_table_df["count"].sum()
            st.map(space_table_df, size="count", color="color", use_container_width=True)
        else:
            st.map(space_table_df, size=25, color="color", use_container_width=True)