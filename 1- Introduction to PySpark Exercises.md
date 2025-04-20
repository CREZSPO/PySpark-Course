Welcome! In this course, we’ll explore [[PySpark]], a powerful tool for processing and analyzing big data. Designed for data engineers, data scientists, and machine learning enthusiasts, this course will teach you to work with large-scale datasets in distributed environments, transforming raw data into valuable insights.

[[Apache]] Spark is an open-source, distributed computing system designed for fast processing of large-scale data. PySpark is the Python interface for Apache Spark. It handles large datasets efficiently with parallel computation in Python workflows, ideal for batch processing, real-time streaming, machine learning, data analytics, and SQL querying. PySpark supports industries like finance, healthcare, and e-commerce with speed and scalability.

A key component of working with PySpark is clusters. A Spark cluster is a group of computers (nodes) that collaboratively process large datasets using Apache Spark, with a master node coordinating multiple worker nodes. This architecture enables distributed processing. The master node manages resources and tasks, while worker nodes execute assigned compute tasks.

A SparkSession is the entry point into PySpark, enabling interaction with Apache Spark's core capabilities. It allows us to execute queries, process data, and manage resources in the Spark cluster. To create a SparkSession, run `from pyspark.sql import SparkSession`. We’ll create a session named MySparkApp using `SparkSession.builder`, stored as the variable spark. The `.builder()` method sets up the session, while `getOrCreate()` initiates a new session or retrieves an existing one. The `.appName()` method helps manage multiple PySpark applications. With our SparkSession ready, we can load data and apply transformations or actions. It’s best practice to use `SparkSession.builder.getOrCreate()`, which returns an existing session or creates a new one if necessary.

PySpark is ideal for handling large datasets that can’t be processed on a single machine. It excels in: Big Data Analytics through Distributed Data Processing, using Spark’s in-memory computation for faster processing. Machine Learning on Large Datasets leverages Spark’s MLlib for scalable model training and evaluation. ETL and ELT Pipelines transforms large volumes of raw data from sources into structured formats. PySpark is flexible, working with diverse data sources like CSVs, Parquet, and many more.
# Introduction to Apache Spark and PySpark
## Creating a SparkSession

Let's start with creating a new `SparkSession`. In this course, you will be usually provided with one, but creating a new one or getting an existing one is a must-have skill!

```python
# Import SparkSession from pyspark.sql

from pyspark.sql import SparkSession

# Create my_spark

my_spark = SparkSession.builder.appName("my_spark").getOrCreate()
  
# Print my_spark

print(my_spark)
```

##  Loading census data

Let's start creating your first PySpark DataFrame! The file `adult_reduced.csv` contains a grouping of adults based on a variety of demographic categories. These data have been adapted from the US Census. There are a total of 32562 groupings of adults.

We should load the csv and see the resulting schema.

Data dictionary:

| Variable       | Description         |
| -------------- | ------------------- |
| age            | Individual age      |
| education_num  | Education by degree |
| marital_status | Marital status      |
| occupation     | Occupation          |
| income         | Categorical income  |

```python
# Read in the CSV

census_adult = spark.read.csv("adult_reduced.csv")

# Show the DataFrame

census_adult.show()
```

## Reading a CSV and performing aggregations

You have a spreadsheet of Data Scientist salaries from companies ranging is size from small to large. You want to see if there is a major difference between average salaries grouped by company size.

Remember, there's already a `SparkSession` called `spark` in your workspace!

```python
# Load the CSV file into a DataFrame

salaries_df = spark.read.csv("salaries.csv", header=True, inferSchema=True)

# Count the total number of rows

row_count = salaries_df.count()
print(f"Total rows: {row_count}")

# Group by company size and calculate the average of salaries

salaries_df.groupBy("company_size").agg({"salary_in_usd": "avg"}).show()
salaries_df.show()
```

## Filtering by company

Using that same dataset from the last exercise, you realized that you only care about the jobs that are entry level (`"EN"`) in Canada (`"CA"`). What does the salaries look like there? Remember, there's already a `SparkSession` called `spark` in your workspace!

```python
# Average salary for entry level in Canada
CA_jobs = ca_salaries_df.filter(ca_salaries_df['company_location'] == "CA").filter(ca_salaries_df['experience_level'] == "EN").groupBy().avg("salary_in_usd")

# Show the result

CA_jobs.show()
```
## Infer and filter

Imagine you have a census dataset that you know has a header and a schema. Let's load that dataset and let PySpark infer the schema. What do you see if you filter on adults over 40?

Remember, there's already a `SparkSession` called `spark` in your workspace!

```python
# Load the dataframe

census_df = spark.read.json("adults.json")

# Filter rows based on age condition

salary_filtered_census = census_df.filter(census_df['age'] > 40)

# Show the result

salary_filtered_census.show()
```

## Schema writeout

We've loaded Schemas multiple ways now. So lets define a schema directly. We'll use a Data dictionary:

|Variable|Description|
|---|---|
|age|Individual age|
|education_num|Education by degree|
|marital_status|Marital status|
|occupation|Occupation|
|income|Categorical income|
```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Fill in the schema with the columns you need from the exercise instructions

schema = StructType([StructField("age",IntegerType()),
                     StructField("education_num",IntegerType()),
                     StructField("marital_status",StringType()),
                     StructField("occupation",StringType()),
                     StructField("income",StringType()) ])

# Read in the CSV, using the schema you defined above

census_adult = spark.read.csv("adult_reduced_100.csv", sep=',', header=False, schema=schema)  

# Print out the schema

census_adult.printSchema()

```

# PySpark in Python

## Handling missing data with fill and drop

Oh my… You have a lot of missing values in this dataset! Let's clean it up! With the loaded CSV file, drop rows with any null values, and show the results!

Remember, there's already a `SparkSession` called `spark` in your workspace!

```python
# Drop rows with any nulls

census_cleaned = census_df.na.drop()

# Show the result

census_cleaned.show()
```

## Column operations - creating and renaming columns

The `census` dataset is still not quite showing everything you want it to. Let's make a new synthetic column by adding a new column based on existing columns, and rename it for clarity.

Remember, there's already a `SparkSession` called `spark` in your workspace!

```python
# Create a new column 'weekly_salary'

census_df_weekly = census_df.withColumn('weekly_salary', census_df['income'] / 52)

# Rename the 'age' column to 'years'

census_df_weekly = census_df_weekly.withColumnRenamed("age", "years")

# Show the result

census_df_weekly.show()
```

## Joining flights with their destination airports

You've been hired as a data engineer for a global travel company. Your first task is to help the company improve its operations by analyzing flight data. You have two datasets in your workspace: one containing details about flights (`flights`) and another with information about destination airports (`airports`), both are already available in your workspace..

Your goal? Combine these datasets to create a powerful dataset that links each flight to its destination airport.

```python
# Examine the data
airports.show()

# .withColumnRenamed() renames the "faa" column to "dest"

airports = airports.withColumnRenamed("faa", "dest")

# Join the DataFrames

flights_with_airports = flights.join(airports, on='dest', how='leftouter')  

# Examine the new DataFrame

flights_with_airports.show()
```

## Integers in PySpark UDFs

This exercise covers UDFs, allowing you to understand function creation in PySpark! As you work through this exercise, think about what this would replace in a data cleaning workflow.

Remember, there's already a `SparkSession` called `spark` in your workspace!

```python
# Register the function age_category as a UDF

age_category_udf = udf(age_category, StringType())

# Apply your udf to the DataFrame

age_category_df_2 = age_category_df.withColumn("category", age_category_udf(age_category_df["age"]))

# Show df

age_category_df_2.show()
```

## Pandas UDFs

This exercise covers Pandas UDFs, so that you can practice their syntax! As you work through this exercise, notice the differences between the Pyspark UDF from the last exercise and this type of UDF.

Remember, there's already a `SparkSession` called `spark` in your workspace!

```python 
# Define a Pandas UDF that adds 10 to each element in a vectorized way

@pandas_udf(DoubleType())
def add_ten_pandas(column):
    return column + 10

# Apply the UDF and show the result

df.withColumn("10_plus", add_ten_pandas(df['value']))
df.show()

```

# Introduction to PySpark SQL

## Creating RDDs

In PySpark, you can create an RDD (Resilient Distributed Dataset) in a few different ways. Since you are already familiar with DataFrames, you will set this up using a DataFrame. Remember, there's already a `SparkSession` called `spark` in your workspace!

```python
# Create a DataFrame
df = spark.read.csv("salaries.csv", header=True, inferSchema=True)

# Convert DataFrame to RDD
rdd = df.rdd

# Show the RDD's contents
rdd.collect()
print(rdd)
```

## Collecting RDDs
 
For this exercise, you’ll work with both RDDs and DataFrames in PySpark. The goal is to group data and perform aggregation using both RDD operations and DataFrame methods.

You will load a CSV file containing employee salary data into PySpark as an RDD. You'll then group by the experience level data and calculate the maximum salary for each experience level from a DataFrame. By doing this, you'll see the relative strengths of both data formats.

The dataset you're using is related to Data Scientist Salaries, so finding market trends are in your best interests! We've already loaded and normalized the data for you! Remember, there's already a `SparkSession` called `spark` in your workspace! 

```python
# Create an RDD from the df_salaries
rdd_salaries = df_salaries.rdd

# Collect and print the results
print(rdd_salaries.collect())

# Group by the experience level and calculate the maximum salary
dataframe_results = df_salaries.groupby("experience_level").agg({"salary_in_usd": 'max'})

# Show the results
dataframe_results.show()
```

Great job! Not surprisingly, more experienced (Senior and Staff level) are earning more! As you can see, it is possible to use both RDDs and Dataframes with PySpark on the same data. As you work with larger and more complex datasets, you'll find that RDDs may be faster and more efficient to move and process, often using lambda functions, but DataFrames have a simpler syntax and will allow you to do analytics easily! As with many things with PySpark, you need to evaluate your problem and data early to find the optimal path.

## Querying on a temp view

In this exercise, you'll practice registering a DataFrame as a temporary SQL view in PySpark. Temporary views are powerful tools that allow you to query data using SQL syntax, making complex data manipulations easier and more intuitive. Your goal is to create a view from a provided DataFrame and run SQL queries against it, a common task for ETL and ELT work.

You already have a SparkContext, `spark`, and a PySpark DataFrame, `df`, available in your workspace.

```python
# Register as a view

df.createOrReplaceTempView("data_view")

# Advanced SQL query: Calculate total salary by Position

result = spark.sql("""
    SELECT Position, SUM(Salary) AS Total_Salary
    FROM data_view
    GROUP BY Position
    ORDER BY Total_Salary DESC
    """
)
result.show()
```

## Running SQL on DataFrames

DataFrames can be easily manipulated using SQL queries in PySpark. The `.sql()` method in a SparkSession enables applications to run SQL queries programmatically and returns the result as another DataFrame. In this exercise, you'll create a temporary table of a DataFrame that you have created previously, then construct a query to select the names of the people from the temporary table and assign the result to a new DataFrame.

Remember, you already have a SparkSession `spark` and a DataFrame `df` available in your workspace.

 ```python
 # Create a temporary table "people"
df.createOrReplaceTempView("people")

# Select the names from the temporary table people
query = """SELECT name FROM people"""

# Assign the result of Spark's query to people_df_names
people_df_names = spark.sql(query)

# Print the top 10 names of the people
people_df_names.show(10)
``` 

## Analytics with SQL on DataFrames

SQL queries are concise and easy to run compared to DataFrame operations. But in order to apply SQL queries on a DataFrame first, you need to create a temporary view of the DataFrame as a table and then apply SQL queries on the created table.

You already have a SparkContext `spark` and `salaries_df` available in your workspace.

```python
# Create a temporary view of salaries_table
salaries_df.createOrReplaceTempView('salaries_table')

# Construct the "query"
query = '''SELECT job_title, salary_in_usd FROM salaries_table WHERE company_location == "CA"'''

# Apply the SQL "query"
canada_titles = spark.sql(query)

# Generate basic statistics
canada_titles.describe().show()
```

## Aggregating in PySpark

Now you're ready to do some aggregating of your own! You're going to use a salary dataset that you have already used. Let's see what aggregations you can create! A SparkSession called `spark` is already in your workspace, along with the Spark DataFrame `salaries_df`.

```python
# Find the minimum salaries for small companies
salaries_df.filter(salaries_df.company_size == "S").groupBy().min("salary_in_usd").show()

# Find the maximum salaries for large companies
salaries_df.filter(salaries_df.company_size == "L").groupBy().max("salary_in_usd").show()
```

## Aggregating in RDDs

Now that you have conducted analytics with DataFrames in PySpark, let's briefly do a similar task with an RDD. Using the provided code, get the sum of the values of an RDD in PySpark.

A Spark session called `spark` has already been made for you.

```python
# DataFrame Creation
data = [("HR", "3000"), ("IT", "4000"), ("Finance", "3500")]
columns = ["Department", "Salary"]
df = spark.createDataFrame(data, schema=columns)

# Map the DataFrame to an RDD
rdd = df.rdd.map(lambda row: (row["Department"], row["Salary"]))

# Apply a lambda function to get the sum of the DataFrame
rdd_aggregated = rdd.reduceByKey(lambda x, y: x + y)

# Show the collected Results
print(rdd_aggregated.collect())
``` 

##  Complex Aggregations

To get you familiar with more of the built in aggregation methods, let's do a slightly more complex aggregation! The goal is to merge all these commands into a single line.

Remember, a SparkSession called `spark` is already in your workspace, along the Spark DataFrame `salaries_df`.

```python
# Average salaries at large us companies
large_companies=salaries_df.filter(salaries_df.company_size == "L").filter(salaries_df.company_location == "US").groupBy().avg("salary_in_usd")

#set a large companies variable for other analytics
large_companies=salaries_df.filter(salaries_df.company_size == "L").filter(salaries_df.company_location == "US")

# Total salaries in usd
large_companies.groupBy().sum("salary_in_usd").show()
```

## Broadcasting

What is the purpose of broadcasting in PySpark?

> To avoid data shuffling for small datasets in joins

## Bringing it all together I

You've built a solid foundation in PySpark, explored its core components, and worked through practical scenarios involving Spark SQL, DataFrames, and advanced operations. Now it’s time to bring it all together. Over the next two exercises, you're going to make a SparkSession, a Dataframe, cache that Dataframe, conduct analytics and explain the outcome!

```python
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.appName("final_spark").getOrCreate()

# Print my_spark
print(my_spark)

# Load dataset into a DataFrame
df = my_spark.createDataFrame(data, schema=columns)
df.show()
```

##  Bringing it all together II

Create a DataFrame, apply transformations, cache it, and check if it’s cached. Then, uncache it to release memory. For this exercise a `spark` session has been made for you! Look carefully at the outcome of the `.explain()` method to understand what the outcome is!

```python
# Cache the DataFrame
df.cache()

# Perform aggregation
agg_result = df.groupBy("Department").sum("Salary")
agg_result.show()

# Analyze the execution plan
agg_result.explain()

# Uncache the DataFrame
df.unpersist()
```

Congratulations on completing the course! Cacheing and persistance are two concepts that you will get more practice the larger your cluster and data gets! We cached the DataFrame `df` for speed, conducted analytics with `agg_result` for seperation, seeing how it executes, and now we can unpersist the original `df`. We don't need to unpersist `agg_result`, as it was never cached. Mastering PySpark techniques is a major step toward efficient, scalable data processing—skills that will save you time and resources in real-world projects. Keep practicing, and you'll continue to unlock PySpark's full potential!



