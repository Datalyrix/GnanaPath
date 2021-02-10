import logging
import sys
import os
from pyspark.sql import SparkSession

currentDir = os.getcwd()
print('currentDir :'+currentDir)

from gntransforms.testwordcount import testword_count_transformer

LOG_FILENAME = 'gngraphjob.log'
APP_NAME = "gnGraph"

if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
    logging.info(sys.argv)

    if len(sys.argv) is not 3:
        logging.warning("Input .txt file and output path are required")
        sys.exit(1)

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sc = spark.sparkContext
    app_name = sc.appName
    logging.info("Application Initialized: " + app_name)
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    testword_count_transformer.run(spark, input_path, output_path)
    logging.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
