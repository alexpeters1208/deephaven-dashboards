import backend
import streamlit as st

from streamlit_deephaven import display_dh
import deephaven.agg as agg
import deephaven.updateby as uby
import deephaven.time as dhtu
import deephaven.pandas as dhpd
from deephaven.plot.figure import Figure

import datetime as dt

############ APP FRONTEND ############

st.set_page_config(layout="wide")

CTX, tables = backend.create_tables()

# append tables to the streamlit session_state, in hopes that persisting tables through changes helps performance
if len(st.session_state) == 0:
    for var_name, var in tables.items():
        st.session_state[var_name] = var

MIN_DATE = dt.date(2013, 12, 22)
MAX_DATE = dt.date(2017, 8, 1)

available_months_df = dhpd.to_pandas(st.session_state.daily_ride_freq_stats)
AVAILABLE_MONTHS = {year: list(available_months_df.loc[available_months_df["year"] == year]["month"])
                    for year in (2013, 2014, 2015, 2016, 2017)}

MONTH_STR_TO_INT = {"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
                    "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12}

MONTH_INT_TO_STR = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
                    7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"}

WEEKDAY_STR_TO_INT = {"Monday": 1, "Tuesday": 2, "Wednesday": 3, "Thursday": 4, "Friday": 5, "Saturday": 6,
                      "Sunday": 7}

st.subheader("Austin Bikeshare Exploration")
data_tab, time_tab, space_tab= st.tabs(["Raw Data", "Through Time", "Through Space"])

with data_tab:
    data_c1, data_c2 = st.columns(2)
    with data_c1:
        st.write("_trips_ dataset")
        display_dh(st.session_state.trips)
    with data_c2:
        st.write("_stations_ dataset")
        display_dh(st.session_state.stations)

    st.write("Input your own Deephaven query here!")
    st.text_input("This only supports single-expression queries.",
                  "trips.update(\"month = monthOfYear(start_time, 'CT')\").where(\"month == 3\")",
                  key = "data_query")
    with CTX:
        custom_query_table = eval("st.session_state." + st.session_state.data_query)
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
            with CTX:
                daily_frequency_plot = Figure().\
                    plot_xy(series_name="Daily ride count",
                            t=st.session_state.daily_ride_freq_avg.where(["toLocalDate(timestamp, 'CT') >= primary_frequency_plot_start_j",
                                                         "toLocalDate(timestamp, 'CT') <= primary_frequency_plot_end_j"]),
                            x="timestamp",
                            y="trip_count").\
                    plot_xy(series_name="30-day rolling average",
                            t=st.session_state.daily_ride_freq_avg.where(["toLocalDate(timestamp, 'CT') >= primary_frequency_plot_start_j",
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
            with CTX:
                hourly_frequency_plot = Figure(). \
                    plot_xy(series_name="Hourly ride count",
                            t=st.session_state.hourly_ride_freq_avg.where(["year == (int)st.session_state.freq_year",
                                                          "month == (int)MONTH_STR_TO_INT[st.session_state.freq_month]"]),
                            x="timestamp",
                            y="trip_count"). \
                    plot_xy(series_name="24-hour rolling average",
                            t=st.session_state.hourly_ride_freq_avg.where(["year == (int)st.session_state.freq_year",
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
            with CTX:
                daily_duration_plot = Figure().\
                    plot_xy(series_name=daily_title,
                            t=st.session_state.daily_ride_dur_avg.where(["toLocalDate(timestamp, 'CT') >= primary_duration_plot_start_j",
                                                         "toLocalDate(timestamp, 'CT') <= primary_duration_plot_end_j"]),
                            x="timestamp",
                            y=daily_stat_column).\
                    plot_xy(series_name="30-day rolling average",
                            t=st.session_state.daily_ride_dur_avg.where(["toLocalDate(timestamp, 'CT') >= primary_duration_plot_start_j",
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
            with CTX:
                hourly_duration_plot = Figure(). \
                    plot_xy(series_name=hourly_title,
                            t=st.session_state.hourly_ride_dur_avg.where(["year == (int)st.session_state.dur_year",
                                                          "month == (int)MONTH_STR_TO_INT[st.session_state.dur_month]"]),
                            x="timestamp",
                            y=hourly_stat_column). \
                    plot_xy(series_name="24-hour rolling average",
                            t=st.session_state.hourly_ride_dur_avg.where(["year == (int)st.session_state.dur_year",
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
            with CTX:
                time_q1_plot = Figure(). \
                    plot_pie(series_name="Ride Frequency by Subscription Type",
                             t=st.session_state.trips.count_by("count", by="subscriber_type"),
                             category="subscriber_type",
                             y="count"). \
                    show()
            display_dh(time_q1_plot)

        with st.expander("How does overall ride count trend compare from year to year?"):
            st.write("This plot shows trends from all three complete years. The differences in magnitude \
                     and variation have been removed to make the trend comparison as simple as possible.")
            with CTX:
                time_q2_plot = Figure(). \
                    plot_xy(series_name="Year-standardized trip count in 2014",
                            t=st.session_state.daily_ride_freq_avg.where("year == 2014").update("day_of_year = dayOfYear(timestamp, 'CT')"),
                            x="day_of_year",
                            y="standardized_trip_count_avg"). \
                    plot_xy(series_name="Year-standardized trip count in 2015",
                            t=st.session_state.daily_ride_freq_avg.where("year == 2015").update("day_of_year = dayOfYear(timestamp, 'CT')"),
                            x="day_of_year",
                            y="standardized_trip_count_avg"). \
                    plot_xy(series_name="Year-standardized trip count in 2016",
                            t=st.session_state.daily_ride_freq_avg.where("year == 2016").update("day_of_year = dayOfYear(timestamp, 'CT')"),
                            x="day_of_year",
                            y="standardized_trip_count_avg"). \
                    show()
            display_dh(time_q2_plot)

        with st.expander("Do the same months exhibit similar ride count trends from year to year?"):
            st.write("This plot shows trends from the selected month from all three complete years. The differences \
                     in magnitude and variation have been removed to make the trend comparison as simple as possible.")
            st.selectbox("Select a month.", (MONTH_STR_TO_INT.keys()), key = "freq_q3_month")
            with CTX:
                time_q3_plot = Figure(rows=1, cols=3). \
                    new_chart(row=0, col=0). \
                    plot_xy(series_name="2014",
                            t=st.session_state.hourly_ride_freq_avg.where(["year == 2014", "month == (int)MONTH_STR_TO_INT[st.session_state.freq_q3_month]"]),
                            x="timestamp",
                            y="standardized_trip_count_avg"). \
                    new_chart(row=0, col=1). \
                    plot_xy(series_name="2015",
                            t=st.session_state.hourly_ride_freq_avg.where(["year == 2015", "month == (int)MONTH_STR_TO_INT[st.session_state.freq_q3_month]"]),
                            x="timestamp",
                            y="standardized_trip_count_avg"). \
                    new_chart(row=0, col=2). \
                    plot_xy(series_name="2016",
                            t=st.session_state.hourly_ride_freq_avg.where(["year == 2016", "month == (int)MONTH_STR_TO_INT[st.session_state.freq_q3_month]"]),
                            x="timestamp",
                            y="standardized_trip_count_avg"). \
                    show()
            display_dh(time_q3_plot)

        with st.expander("Which months have the highest ride counts? Which have the lowest?"):
            st.write("This plot shows the ride count by month for a given year. If all years are selected, only complete \
                     years will be included in the plot.")
            with CTX:
                time_q4_plot = Figure() \
                    .plot_cat(series_name="2013",
                              t=st.session_state.daily_ride_freq.where("year == 2013").agg_by(agg.sum_("trip_count"), by="month"),
                              category="month", y="trip_count") \
                    .plot_cat(series_name="2014",
                              t=st.session_state.daily_ride_freq.where("year == 2014").agg_by(agg.sum_("trip_count"), by="month"),
                              category="month", y="trip_count") \
                    .plot_cat(series_name="2015",
                              t=st.session_state.daily_ride_freq.where("year == 2015").agg_by(agg.sum_("trip_count"), by="month"),
                              category="month", y="trip_count") \
                    .plot_cat(series_name="2016",
                              t=st.session_state.daily_ride_freq.where("year == 2016").agg_by(agg.sum_("trip_count"), by="month"),
                              category="month", y="trip_count") \
                    .plot_cat(series_name="2017",
                              t=st.session_state.daily_ride_freq.where("year == 2017").agg_by(agg.sum_("trip_count"), by="month"),
                              category="month", y="trip_count") \
                    .show()
            display_dh(time_q4_plot)

        with st.expander("How does subscription status affect the overall distribution of ride duration?"):
            st.write("This plot shows the distribution of ride duration for annual subscribers and everyone else. \
                     Interestingly, annual subscribers tend to take shorter trips.")
            with CTX:
                time_q5_plot = Figure(rows=2, cols=1). \
                    new_chart(row=0, col=0). \
                    plot_xy_hist(series_name="Annual subscribers",
                                 t=st.session_state.trips.where("subscriber_type == `Annual Membership`"),
                                 x="duration_minutes",
                                 nbins=50,
                                 xmin=0.0,
                                 xmax=100.0). \
                    new_chart(row=1, col=0). \
                    plot_xy_hist(series_name="Non-annual subscribers",
                                 t=st.session_state.trips.where("subscriber_type != `Annual Membership`"),
                                 x="duration_minutes",
                                 nbins=50,
                                 xmin=0.0,
                                 xmax=100.0). \
                    show()
            display_dh(time_q5_plot)

        with st.expander("What percentage of trips over 500 minutes are taken by different types of subscribers?"):
            st.write("This plot shows the percentage of trips over 500 minutes taken by each subscription type. The \
                     majority of long trips are taken by walk-up customers.")
            with CTX:
                time_q6_plot = Figure(). \
                    plot_pie(series_name="Long Rides by Subscription Type",
                             t=st.session_state.trips.where("duration_minutes > 500").count_by("count", by="subscriber_type"),
                             category="subscriber_type",
                             y="count"). \
                    show()
            display_dh(time_q6_plot)

        with st.expander("Do users tend to take longer rides on the weekends?"):
            st.write("This plot shows the average and median number of rides taken each day of the week, aggregated \
                     over the entire dataset. ")
            day_of_week_name_array = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

            with CTX:
                time_q7_table = st.session_state.trips. \
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

            with CTX:
                time_q8_table = st.session_state.trips. \
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
    with CTX:
        stations_color = st.session_state.stations.update("color = status == `active` ? `#1EB025` : status == `closed` ? `#DF0B0B` : status == `moved` ? `#CDC70E` : `#1859EE`")

    with space_c1:
        st.multiselect("Select a station type.", ("active", "closed", "moved", "ACL only"),
                       default = ("active", "closed", "moved", "ACL only"), key = "space_type")
        st.radio("Size station points by:", ("None", "Starting point popularity", "End point popularity"), key = "space_size")
        st.radio("Select a year.", ("All", 2013, 2014, 2015, 2016, 2017), key = "space_year", horizontal = True)
        selectable_months = ["All", *[MONTH_INT_TO_STR[month] for month in AVAILABLE_MONTHS[st.session_state.space_year]]] \
            if st.session_state.space_year != "All" else ["All", *MONTH_STR_TO_INT.keys()]
        st.selectbox("Select a month.", selectable_months, key = "space_month")

        with CTX:
            if len(st.session_state.space_type) == 4:
                space_table = stations_color
            else:
                space_table = stations_color.where_one_of(["status == `" + station_type + "`" for station_type in st.session_state.space_type])
            if st.session_state.space_size != "None":
                filtered_trips = st.session_state.trips
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
        with CTX:
            space_table_df = dhpd.to_pandas(space_table)
            if st.session_state.space_size != "None":
                space_table_df["count"] = 2000 * space_table_df["count"] / space_table_df["count"].sum()
                st.map(space_table_df, size="count", color="color", use_container_width=True)
            else:
                st.map(space_table_df, size=25, color="color", use_container_width=True)