from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, sum, format_number

# Initialize Spark session and done
spark = SparkSession.builder \
    .appName("Transform") \
    .master("local[*]") \
    .getOrCreate()

# Path to the CSV file
path = "/tmp/bigdata_nov_2024/mucteba/people_data.csv"

# Load the CSV data
dataframe = spark.read.csv(path, header=True, inferSchema=True)

# Show the loaded data
dataframe.show()

# Rename columns if necessary
dataframe = dataframe.withColumnRenamed('First Name', 'First_Name') \
    .withColumnRenamed('Last Name', 'Last_Name') \
    .withColumnRenamed('Date of birth', 'Date_of_Birth') \
    .withColumnRenamed('Job Title', 'Job_Title')

# Show the dataframe after renaming
dataframe.show()

# Show the column data types
print(dataframe.dtypes)

# Filter the data for a specific condition (e.g., Male entries)
dataframe_filter = dataframe.filter(col('Sex') == 'Male')
dataframe_filter.show(5)

# Add a new column 'Full Name' by concatenating 'First Name' and 'Last Name'
dataframe = dataframe.withColumn('Full Name', col('First_Name') + ' ' + col('Last_Name'))
dataframe.show(5)

# Drop columns (for example, 'Email' and 'Date_of_Birth')
dataframe_dropped = dataframe.drop('Email', 'Date_of_Birth')
dataframe_dropped.show()

# Group By and Aggregate (e.g., count people by 'Job Title')
grouped_by_job_title = dataframe.groupBy('Job_Title').count()
grouped_by_job_title.show()

# Format numbers (e.g., 'Date_of_Birth' to a formatted string)
formatted_date_of_birth = dataframe.withColumn('Formatted_DOB', format_number(col('Date_of_Birth').cast('timestamp'), 2))
formatted_date_of_birth.show()

# Sort the dataframe by 'Job_Title'
sorted_df = dataframe.orderBy('Job_Title')
sorted_df.show()

# Replace values in 'Sex' column (e.g., 'Male' -> 1, 'Female' -> 0)
replaced_sex = dataframe.withColumn('Sex_Num', when(col('Sex') == 'Male', 1).otherwise(0))
replaced_sex.show()

# Example of grouping by 'Job Title' and aggregating (e.g., count by job title)
group_by_job = dataframe.groupBy('Job_Title').agg(count('First_Name').alias('Count'))
group_by_job.show()
