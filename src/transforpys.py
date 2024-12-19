from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, avg
from pyspark.sql.functions import col, lit, when, sum, avg
from pyspark.sql.functions import sum, format_number

spark = SparkSession.builder \
    .appName("Transfrom") \
    .master("local[*]") \
    .getOrCreate()
path= "/tmp/bigdata_nov_2024/mucteba//sales.csv"
dataframe = spark.read.csv(path,header = True, inferSchema = True)
dataframe.show()
# rename the column
dataframe = dataframe.withColumnRenamed('Order Date','Purchase Date')
dataframe.show()
dataframe.dtypes
# Filter Data Based on Condition
dataframefilter = dataframe.filter(col('Item Type') == 'Household')
dataframefilter.show(5)
# rename a column
dataframe1 = dataframe.withColumnRenamed('Total Revenue', 'Total Sales')
dataframe2 = dataframe1.withColumnRenamed('Total Cost', 'Total Purchase')
dataframe2.show()
# Add a new column

dataframe = dataframe.withColumn('Total sales', col('Unit Price') * col('Units Sold'))
dataframe.show(5)
dataframe2.dtypes
# drop a column

dataframe3 = dataframe2.drop('Total Revenue')
#drop orderid
dataframe4 = dataframe3.drop('Order ID')
# Group By and Aggregate
groupdf = dataframe4.groupBy('Order Priority').agg(sum('Total sales'))
groupdf.show()
formatted_df = dataframe4.groupBy("Order Priority") \
    .agg(sum("Total Sales").alias("Total_Sales_Sum")) \
    .withColumn("Formatted_Total_Sales", format_number("Total_Sales_Sum", 2)) \
    .select("Order Priority", "Formatted_Total_Sales")
formatted_df.show()

# group by revenue
revenue = dataframe4.groupBy('Region').sum('Total Sales')
revenue.show()
formatreg = revenue.withColumn("sum(Total Sales)", format_number(col("sum(Total Sales)"),2))
formatreg.show()
# sort data by column
sortdf = dataframe4.orderBy('Order Priority')
sortdf.show()
# replaced value in column
relaced = dataframe4.withColumn('Sales Channel', when(col('Sales Channel') == 'online',1). otherwise(0))
relaced.show()
#group by sales
groupchannel = dataframe4.groupBy("Sales Channel").agg(sum("Total Sales").alias('Total Salessum'))
groupchannel.show()
formatted_df = groupchannel.withColumn("Total Salessum", format_number(col("Total Salessum"), 2))
formatted_df.show()






