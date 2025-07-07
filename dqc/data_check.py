from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, initcap, count, trim

#Intializing Spark
spark = SparkSession.builder.appName("Data_Quality_Checks").getOrCreate()

#Loading Data
orders = spark.read.csv("/Users/revanth/Downloads/orders_dirty.csv", header=True, inferSchema=True)
products = spark.read.csv("/Users/revanth/Downloads/products_dirty.csv", header=True, inferSchema=True)
customers = spark.read.csv("/Users/revanth/Downloads/customers_dirty.csv", header=True, inferSchema=True)

#Trimming Strings
products = products.withColumn("product_name", initcap(trim(col("product_name"))))
customers = customers.withColumn("customer_name", initcap(trim(col("customer_name"))))

#Joining Datasets
df = orders.join(products, "product_id", "left").join(customers, "customer_id", "left")

#Calculating Order Value and adding new column order_value
df = df.withColumn("order_value", round(col("quantity") * col("price"), 2))

#Adding new column Loyalty Tier
df = df.withColumn("loyalty_tier", when(col("order_value") >= 1000, "Platinum").when(col("order_value") >= 500, "Gold").when(col("order_value") >= 100, "Silver").otherwise("Bronze"))

#Adding Dataframe into Memory
df.cache()

#Raw Data
print("Raw Data:")
df.show(truncate=False)

#Checking Null
print("Null Check:") 
df1 = df.select([
    count(when(col(c).isNull(), c)).alias(c + "_nulls") 
    for c in df.columns
])
df1.show()

#Checking Empty String
print("Empty String Check:")
df2 = df.select([count(when(trim(col(c)) == "", c)).alias(c + "_empty") for c in ["product_name", "customer_name"]])
df2.show()

#Invalid Price
print("Invalid Price:")
df3 = df.filter((col("price").isNull()) | (col("price") <= 0))
df3.show()

#Invalid Emails
print("Invalid Emails:")
df4 = df.filter(~col("email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"))
df4.show()

#Invalid Categories
print("Invalid Categories:")
allowed_cat = ["electronics", "furniture", "stationery"]
df5 = df.filter(~col("category").isin(allowed_cat))
df5.show()

#Summary of Invalid Records
invalid_sum = df.filter((col("price").isNull()) | (col("price") <= 0) | (~col("email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$")) | (~col("category").isin(allowed_cat)))
print("Summary of Invalid Records:")
invalid_sum.show()

#Cleaned Dataset
df_clean = df.filter((col("price") > 0) & (col("email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$")) & (col("category").isin(allowed_cat)))
print("Cleaned Dataset:")
df_clean.show()

#Saving invalid_sum & df_clean in parquet form
df_clean.write.mode("overwrite").parquet("output/clean_dataset/")
invalid_sum.write.mode("overwrite").parquet("output/invalid_sum/")

spark.stop()

 
 
