from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


conf = SparkConf().setAppName("app").setMaster("local[2]")
sc = SparkContext(conf=conf)

data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
print(distData.count())

spark=SparkSession.builder.master("local[4]").appName("demo").getOrCreate()


df = spark.read.parquet("C:\\Users\\ahmed\\IdeaProjects\\SparkApps\\src\\main\\resources\\\output")

df.show(25)