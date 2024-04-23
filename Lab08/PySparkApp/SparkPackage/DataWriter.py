class DataWriter:
    def __init__(self, spark):
        self.spark = spark

    def write_csv(self, df, path):
        df.coalesce(1).write.format("csv") \
            .mode("overwrite") \
            .option("header", "true") \
            .save(path)
        df.write.csv(path, header=True, mode="overwrite")