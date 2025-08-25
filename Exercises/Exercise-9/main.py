import polars as pl
import os

def load_csv(file_path: str):
    return pl.read_csv(source=file_path,
                       columns=["started_at"],
                       schema_overrides={"ride_id": pl.String,
                                         "rideable_type": pl.String,
                                         "started_at": pl.Datetime,
                                         "ended_at": pl.Datetime,
                                         "start_station_name": pl.String,
                                         "start_station_id": pl.UInt32,
                                         "end_station_name": pl.String,
                                         "end_station_id": pl.UInt32,
                                         "start_lat": pl.Float32,
                                         "start_lng": pl.Float32,
                                         "end_lat": pl.Float32,
                                         "end_lng": pl.Float32,
                                         "member_casual": pl.Categorical})

def rides_per_day(dataframe):
    return dataframe.with_columns(pl.col("started_at").dt.date().alias("started_date")).group_by("started_date").count()

def aggregate_rides_per_week(dataframe):
    df_agg = dataframe.with_columns(pl.col("started_at").dt.week()).group_by("started_at").agg(cnt=pl.col("started_at").count())
    return df_agg.select(pl.mean("cnt").alias("mean"),
                         pl.min("cnt").alias("min"),
                         pl.max("cnt").alias("max"))

def different_to_last_week(dataframe):
    mod_df = rides_per_day(dataframe)
    return mod_df.sort("started_date") \
            .with_columns(pl.col("count").cast(pl.Int32)) \
            .with_columns(pl.col("count").shift(7).alias("count_last_week")) \
            .select(pl.col("started_date"), pl.col("count"), pl.col("count_last_week"), (pl.col("count") - pl.col("count_last_week")).alias("diff_to_last_week"))

def main():
    file_path = os.path.join(os.getcwd(), "data\\202306-divvy-tripdata.csv")
    df = load_csv(file_path)
    print(rides_per_day(df))
    print(aggregate_rides_per_week(df))
    print(different_to_last_week(df).tail(10))

if __name__ == "__main__":
    main()
