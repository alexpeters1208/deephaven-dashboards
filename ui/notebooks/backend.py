from deephaven import read_csv
import deephaven.agg as agg
import deephaven.updateby as uby
import deephaven.time as dhtu
import deephaven.pandas as dhpd

# other imports
import datetime as dt

stations = read_csv("/data/austin_bikeshare_stations.csv")
trips = read_csv("/data/austin_bikeshare_trips.csv")

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
