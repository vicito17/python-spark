from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([ \
                     StructField("Consumidor", IntegerType(), True), \
                     StructField("Producto", IntegerType(), True), \
                     StructField("Precio", FloatType(), True) \
                    ])

df = spark.read.schema(schema).csv("data/customer-orders.csv")
df.printSchema()
df.groupBy("Consumidor").agg(func.round(func.sum("Precio"),2).alias("Gasto total"),
                             func.round(func.avg("Precio")).alias("Gasto medio"),
                             func.count("Consumidor").alias("Número de productos comprados")) \
                        .orderBy(func.desc("Gasto total")).show()

spark.stop()