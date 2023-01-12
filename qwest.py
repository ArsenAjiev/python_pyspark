from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, date
from pyspark.sql import Row

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.csv("D:\projects\python_pyspark\qw.csv", header=True)

    df.write.csv("D:\projects\python_pyspark\wwwww.csv", header=True)




    spark.stop()
