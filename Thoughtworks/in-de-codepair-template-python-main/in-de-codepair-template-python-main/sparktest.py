from os import error
import select
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DecimalType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("RDDtoDataFrameSchema").getOrCreate()

    # Create an RDD
data = [("IT", 40), ("HR", 50)]
rdd = spark.sparkContext.parallelize(data)

# Define a schema programmatically
schema = StructType([
    StructField("department", StringType(), True),
    StructField("employee_count", IntegerType(), True)
])

# Convert RDD to DataFrame using createDataFrame with the defined schema
#df_schema = spark.createDataFrame(rdd, schema=schema)
#df_schema.show()
customer_churn_df=spark.read.option("header","True").csv("Churn_Modelling.csv")
customer_detail_df=spark.read.option("header","True").csv("customer_detail.csv")

join_df=customer_churn_df.join(customer_detail_df,customer_churn_df.CustomerId==customer_detail_df.CustomerId,"inner").drop(customer_detail_df.CustomerId)
join_df.show(5)
df=join_df.withColumn("EstimatedSalary",col("EstimatedSalary").cast(DecimalType(13,2))).select("CustomerId","Geography","EstimatedSalary")
df.printSchema()
try:
    print("Total count-->"+ str(df.count()))
    w=Window.partitionBy("Geography").orderBy(desc("EstimatedSalary"))
    new_df=df.withColumn("customer_rank",row_number().over(w))
    final_df=new_df.filter("customer_rank<=10")
    final_df.show()
    print("---------writing data to file--------")
    final_df.write.mode("overwrite").format("csv").option("header","true").save("Output.csv")
    print("----------Yay data written to file----")
except Exception as e:
    print(f"Exception in code----> {e}")
    
finally:
    spark.stop()