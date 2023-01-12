from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("WordCount") \
        .getOrCreate()

    lines = spark.read.text('D:\\new.txt')

    word_count = lines \
        .select(explode(split(lines.value, "\s+"))
                .alias("word")) \
        .groupBy("word") \
        .count()



    print("\n Word Count : ", word_count.show())

    spark.stop()
