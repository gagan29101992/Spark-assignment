from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("covid-analysis"). \
        config("spark.mongodb.input.uri", "mongodb+srv://admin:<password>@<clusterip>/covid_analysis?retryWrites=true&w=majority"). \
        config("spark.mongodb.output.uri", "mongodb+srv://admin:<password>@<clusterip>>/covid_analysis?retryWrites=true&w=majority"). \
		config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"). \
        getOrCreate()


uri = "mongodb+srv://admin:<password>@<clusterip>.vvghdwk.mongodb.net/?retryWrites=true&w=majority"

df_case = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
		  .option( "uri", uri) \
          .option("database", "covid_analysis") \
          .option("collection", "case") \
          .load()

df_region = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
		  .option( "uri", uri) \
          .option("database", "covid_analysis") \
          .option("collection", "region") \
          .load()

df_timeProvince =   spark.read.format("com.mongodb.spark.sql.DefaultSource") \
		            .option( "uri", uri) \
                    .option("database", "covid_analysis") \
                    .option("collection", "timeProvince") \
                    .load()
df_case.show(5)
df_region.show(5)
df_timeProvince.show(5)

print("Number of records in case dataframe are : ", df_case.count())
print("Number of records in region dataframe are : ", df_region.count())
print("Number of records in timeProvince dataframe are : ", df_timeProvince.count())

df_case.describe().show(5)
df_region.describe().show(5)
df_timeProvince.describe().show(5)

print("Dropping duplicate values from dataframes if any: ")

df_case = df_case.dropDuplicates()
df_region = df_region.dropDuplicates()
df_timeProvince = df_timeProvince.dropDuplicates()

print("Dataframes are free from duplicate values now")

print("Showing 10 records from each dataframe using limit function: ")
df_case_limited = df_case.limit(10)
df_region_limited = df_region.limit(10)
df_timeProvince_limited = df_timeProvince.limit(10)

df_case_limited.show()
df_region_limited.show()
df_timeProvince_limited.show()

print("Checking count of null values from the dataframes: ")

df_case_columns = ['case_id','confirmed'] # Choosing integer columns in order to remove null values
df_case.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_case_columns]).show()
df_region_columns = ['academy_ratio','code','elderly_alone_ratio','elderly_population_ratio',
'elementary_school_count','kindergarten_count','nursing_home_count','university_count']
df_region.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_region_columns]).show()

df_timeProvince_columns = ['confirmed','deceased','released']
df_timeProvince.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_timeProvince_columns]).show()

print("Dropping all the records which have null values in any of the columns: ")

df_case.na.drop().show()
df_region.na.drop().show()
df_timeProvince.na.drop().show()

print("Some advanced data analysis on these dataframes: ")

print("Finding total number of cases in each province: ")

df_case.groupBy("province").sum("confirmed").withColumnRenamed("sum(confirmed)", "Total_cases").orderBy(col("Total_cases").desc()).show()

df_timeProvince = df_timeProvince.withColumn("date", col("date").cast(DateType())) # change type of date column from string to date type

df_timeProvince = df_timeProvince.withColumn("month", month(col("date"))) # Extracting month from the dat column as we need to use it in next case.

# Calculate total number of confirmed, released and decesed cases every month of 2020
df_timeProvince.groupBy("month") \
.agg(sum(col("confirmed")).alias("total_confirmed_cases"), sum(col("released")).alias("total_recovered_cases"),sum(col("deceased")).alias("total_deaths")).orderBy("month") \
.show()


# Show records from Daegu province where number of confirmed cases are more than 10.

df_case.filter((col("province") == 'Daegu') & (col("confirmed") > 10)).show()

#Example of using NOT operator in pyspark.

df_case.filter(~(col("province") == 'Daegu') & (col("confirmed") > 10)).show()

#Sorting the dataframe in desecnding order of confirmed cases.
df_case.filter(~(col("province") == 'Daegu') & (col("confirmed") > 10)).show()

#Joining two datasets.

df_case.join(df_region, df_case.province == df_region.province, "left").select(df_case.province, df_case.city, df_region.nursing_home_count).orderBy(col('nursing_home_count').desc()).show()





