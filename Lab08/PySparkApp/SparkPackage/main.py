from pyspark.sql import SparkSession
from DataReader import DataReader
from DataWriter import DataWriter
from DataTransformer import DataFrameTransformer

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Spark_Package").config("spark.driver.memory","2000m").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    data_reader = DataReader(spark)
    df = data_reader.read_csv("../../homes.csv")
    df.show()

    transformer = DataFrameTransformer(spark)
    transformed_df = transformer.count_rooms(df)
    transformed_df.show()

    data_writer = DataWriter(spark)
    data_writer.write_csv(transformed_df, "../../zmodyfikowany_df.csv")