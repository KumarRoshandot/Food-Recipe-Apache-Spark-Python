from Task_Base import Tasks as tasks
from Get_URL_Data import get_recipe
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, expr, udf, split, current_date
from pyspark.sql.types import StringType, IntegerType
from datetime import datetime
import logging
import logging.handlers

import sys
import os

# Required job parameters
recipe_file = sys.argv[1]
tasks_arg = sys.argv[2]
query_table = sys.argv[3]
args_length = len(sys.argv)
#recipe_file = 'C:\\Users\\Dell\\Downloads\\Recipes\\input'
# args_length = 2
if args_length < 2:
    print("Please provide at least 2 arguments as input file location and task to execute")
    exit(1)
else:
    # tasks_arg = 'load,transform,query'
    tasks_piped = [i.strip().lower() for i in tasks_arg.split(',')]
    if 'query' in tasks_piped:
        # query_table = "select * from recipe_table"
        if len(query_table.strip()) == 0:
            print("Please Provide a valid Sql query for Recipe_table(name, recipe, date_of_execution, difficulty)")
            exit(1)

prefix = os.path.abspath(recipe_file)
recipe_parquet_folder = 'Recipe_parquet'
recipe_parquet_outfile = os.path.join(prefix, 'output', recipe_parquet_folder)

start_time = datetime.utcnow()
spark = SparkSession \
    .builder \
    .appName("Recipe Data") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.sql.warehouse.dir", recipe_parquet_outfile) \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# This is to fetch Recipe text info from input url
get_recipe_udf = udf(get_recipe, StringType())


# to get the total time in Minutes
def get_total_time(time_str):
    if 'H' in time_str and 'M' in time_str:
        return int(time_str.split('H')[0])*60 + int((time_str.split('H')[1]).split('M')[0])
    else:
        if 'M' in time_str:
            return int(time_str.split('M')[0])
        else:
            return int(time_str.split('H')[0])*60


# converting it to udf
get_total_time_udf = udf(get_total_time, IntegerType())


# a executor to run some Tasks in a specific order
class Executor(tasks):
    def __init__(self, task=[]):
        self.task = task

    def run(self):
        for i in self.task:
            if i == 'load':
                print('load')
                self.load()
            else:
                if i == 'transform':
                    print('transform')
                    self.transform()
                else:
                    if i == 'query':
                        print('query')
                        self.query_table()
                    else:
                        print("Not a Task")

    def load(self):
        try:
            self.recipe_raw_df = spark.read.json(recipe_file)

        except Exception as e:
            print("Error - during load")
            self.raise_email(str(e))
            raise e

    def transform(self):
        try:
            recipe_raw = self.recipe_raw_df.filter(lower(self.recipe_raw_df["ingredients"]).contains("beef")) \
                .withColumn('cooktime', split(self.recipe_raw_df["cookTime"], 'PT')[1]) \
                .withColumn('preptime', split(self.recipe_raw_df["prepTime"], 'PT')[1])

            recipe_raw = recipe_raw.select(
                recipe_raw["name"],
                get_total_time_udf(recipe_raw["cooktime"]).alias("cookTime"),
                get_total_time_udf(recipe_raw["preptime"]).alias("prepTime"),
                get_recipe_udf(recipe_raw["url"]).alias("recipe"),
                current_date().alias("date_of_execution")
            )
            recipe_raw.createOrReplaceTempView("recipe_flat_view")
            # recipe_raw.show(20, False)
            sql = """
               select 
                 name,
                 recipe,
                 date_of_execution,
                 case when (cookTime + prepTime) > 60 then 'Hard'
                      when (cookTime + prepTime) > 30 and (cookTime + prepTime) <= 60 then 'Medium'
                      when (cookTime + prepTime) <= 30 then 'Easy'
                 else 'Unknown'
                 end as difficulty
                from recipe_flat_view
            """
            recipe_raw = spark.sql(sql)
            recipe_raw.createOrReplaceTempView("recipe_flat_view")
            #final_df.printSchema()
            spark.sql("CREATE DATABASE IF NOT EXISTS mydb")
            spark.sql("use mydb").collect()
            spark.sql("INSERT INTO Recipe_table PARTITION (difficulty)\
                        SELECT name,recipe,date_of_execution,difficulty FROM recipe_flat_view")

        except Exception as e:
            print("Error - during transform")
            self.raise_email(str(e))
            raise e

    def query_table(self):
        try:
            spark.sql("SHOW DATABASES").show()
            spark.sql("use mydb").collect()
            spark.sql("SHOW TABLES").show()
            spark.sql(query_table).show(5, False)

        except Exception as e:
            print("Error - during save")
            self.raise_email(str(e))
            raise e

    def raise_email(self, e):
        smtp_handler = logging.handlers.SMTPHandler(mailhost=("smtp.example.com", 25),
                                                    fromaddr="from@example.com",
                                                    toaddrs="to@example.com",
                                                    subject=u"AppName error!")

        logger = logging.getLogger()
        logger.addHandler(smtp_handler)
        logger.exception(e)


def create_table():
    #recipe_parquet_outfile = 'file:/C:/Users/Dell/Downloads/Recipes/output/Recipe_parquet'
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS mydb")
        spark.sql("SHOW DATABASES").show()
        spark.sql("use mydb").collect()
        spark.sql("SHOW TABLES").show()
        sql_str = "CREATE TABLE IF NOT EXISTS Recipe_table " \
                  "(name STRING, recipe STRING, date_of_execution DATE, difficulty STRING)" \
                  " USING parquet OPTIONS " \
                  "(mode 'append'," \
                  "serialization.format '1'," \
                  "path " + "'"+recipe_parquet_outfile+"'" + ")" \
                  " PARTITIONED BY (difficulty)"
        spark.sql(sql_str)
        spark.sql("SHOW TABLES").show()

    except Exception as e:
        print("Error - outer main")
        executor.raise_email(str(e))
        raise e


if __name__ == "__main__":

    print("log - Job Started. %s" % (datetime.now().strftime('%m/%d/%Y %H:%M:%S')))
    # Task to be run in order
    print(tasks_piped)
    executor = Executor(tasks_piped)

    try:
        create_table()
        executor.run()

    except Exception as e:
        print("Error - outer main")
        executor.raise_email(str(e))
        raise e

    end_time = datetime.utcnow()
    duration = (end_time - start_time).total_seconds()
    print("Log - Job Completed. %s seconds" % duration)
