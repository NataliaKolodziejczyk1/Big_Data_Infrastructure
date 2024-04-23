from pyspark.sql.functions import col


class DataFrameTransformer:
    def __init__(self, spark):
        self.spark = spark

    def count_rooms(self, df):
        transformed_df = df.withColumn("TotalRooms", col("Living") + col("Rooms") + col("Beds") + col("Baths"))
        return transformed_df
