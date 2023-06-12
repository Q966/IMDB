import os
#import pyodbc
#import psycopg2


# Question 1: Make the data available as Parquet

'''
Is pyarrow available on python3 for Linux?
''' 

# Convert tsv to parquet format using pandas
import pandas as pd

def tsv_files_to_parquet(files):

  print('Making files available as parquet ...')

  for file in file_paths:
    output_file = f'{file}.parquet'
    if not os.path.exists(output_file):
      df = pd.read_csv(file, sep="\t")
      df.to_parquet(output_file, engine='auto')
    else:
        print("parquet file already there")

file_paths = ["title.basics.tsv", "title.principals.tsv", "title.ratings.tsv"]
tsv_files_to_parquet(file_paths)

# Question 2: Store data only about movies, in a table in the database of your choice

# With some chance the bash script launched the DDL, and the mysql database is created 


# Fictional session and context
from pyspark import SparkContext, SparkConf #, SQLContext
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("IMDB Data Analysis").getOrCreate()



# df.withColumn('isActor', col('isActor').cast('bool'))

# Create spark dataframes to efficiently store /save it as a persistent table ? As they already have the structure
# Not 
basics_df = spark.read.parquet("basics.parquet", header=True, inferSchema=True)
principals_df = spark.read.parquet("principals.parquet")
ratings_df = spark.read.parquet("ratings.parquet")

basics_df.write.format('odbc').mode('overwrite') \
          .saveAsTable("MOVIES.basics")
principals_df.write.format('odbc').mode('overwrite') \
          .saveAsTable("MOVIES.principals")
ratings_df.write.format('odbc').mode('overwrite') \
          .saveAsTable("MOVIES.ratings")


# Question 3 : Find the actors who made the most films between 2010 and today

sql_actors = '''
  SELECT DISTINCT(p.nconst) from basics b 
  INNER JOIN principals p ON b.tconst = p.tconst
  WHERE p.job = 'actor' 
  AND b.startYear BETWEEN 2010 AND 2023
  ORDER BY COUNT(p.tconst) DESC
  LIMIT 5;
'''
# Question 4 : Find the top 5 movies by genre

sql_top_movies = '''
  SELECT b.originalTitle from basics b
  INNER JOIN ratings r ON b.tconst = r.tconst
  ORDER BY r.averageRating DESC
  GROUP BY b.genres 
  LIMIT 5;
'''

# CONNECT TO YOUR DUMMY MySQL DATABASE IN CLI OR SCRIPT (no security)

HOST = 'HOSTNAME'
DB = 'DBNAME'
USER = 'USERNAME'
PASSWORD = 'PASSWORD'
PORT = 'PORT'

import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="yourusername",
  password="yourpassword",
  database="mydatabase"
) 

try:
  with mydb.cursor() as cursor:
    cursor.execute(sql_actors)
    cursor.execute(sql_top_movies)
except ValueError:
    print("Connection to the DB?")



# Question 5 : Explain how you can industrialize the previous 4 steps, and ingest and expose movie data and statistics on a daily basis

'''
From now on we can automate our daily stats request to show on our website dashboard

Possibly create a workflow (with some scheduled daily request to get the data from IMDB, falling in 'batch processing' category workflow, like with Airflow)
Here there is no authentication process or API to pull data from IMDB (to my knowledge).

So once a day, a script or cronjob in the DAG downloads the data from the website source. It can be necessary to also use distributed spark SQL bulk load function
instead of pandas to integrate parquet files into SQL database for size/perf concerns.

(+The data can be stored on adequate storage services.)
Then briefly verify the date, formats, cleaning if necessary or other tests within a python script that would run on your virtual machine like ec2 or compute engine.
In the script we can merge / insert the data into our database (like Redshift, dynamoDB or BigQuery) with the adequate criteria (like checking if the primary key column
is unique and not null, if the column values are normal and so on)

If not successful, the workflow could send email alerts with the error code
If successful, finally the website script, on the server machine, makes the appropriate db query (usual stats requests as a stored procedure) with the result in 
a pandas/spark Dataframe. 

The stats are then displayed on front end any way you like!

'''

# Question 6 : IMDB exposes the data as an export containing the full data. Explain without implementing it how you can integrate "full" data sources without impacting the website

'''
After downloading the full data and ingesting it in a new table with some criteria, 
our website is still connected with the old database.
Without impacting the website and without creating manually a new machine and deregister the old one,

1. I link a freshly created machine / server (with the same stats queries) to the IMDB full data new database.
2. I save an image of this instance, so I can update the stack of resources with a new image identifier (like AMI)

3. In the stack resource yaml config, allow an extra machine in the Launch Configuration max number of machine
4. Make the Auto Scaling group add a machine with the image that i just created
5. In the Target Group (current servers), deregister the old one (to stop bringing traffic on it from now on)
6. After a while, terminate the old server instance
7. Now all user traffic of our website requests the new instance with full data integrated!

'''



