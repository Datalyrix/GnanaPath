import logging
from pyspark.sql import functions as F

def run(spark, input_path, output_path):
    logging.info("Reading text file from: %s", input_path)
    input_df = spark.read.text(input_path)

    dfWords= ( input_df
          .select(F.lower(F.col("value")).alias("lower_words"))
          .select(F.split(F.col("lower_words")," ").alias("split_words"))
          .select(F.explode(F.col("split_words")).alias("words"))
          .select(F.regexp_extract(F.col("words"), "[a-z]+", 0).alias("word"))
          .where(F.ltrim(F.col("word")) != "")
          .groupBy("word").count().orderBy(F.col("count").desc())
         )

    logging.info("Writing csv to directory: %s", output_path)
    dfWords.show()
    dfWords.coalesce(1).write.mode('overwrite').csv(output_path)
    ####input_df.coalesce(1).write.csv(output_path)
