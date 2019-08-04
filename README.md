# Apache-Spark-Python Recipe data process
Use case for processing a json file in PySpark

1) Below is the requirement

    - build a simple application able to run some Tasks in a specific order. 
	  - To achieve that, you will need an executor taking care of which task to execute, 
	  - and a base Task class with the common abstraction in Python

    As an example:
  
   class Executor(object):

        def __init__(self, tasks=[]):
            self.tasks = tasks
        def run(self):
          # do stuff
          
Pipeline needs to fulfill these tasks:

- Use the dataset on S3 as the input json file
- Do some transformations.
- Create a table and make the output data ready to be queried after and during each execution using Impala, without any manual steps.
- It needs to send an alert if some Task failed, for instance: Send an email, a Slack message or anything else that makes error discovery and     error handling easier. You can create just the abstractions for that, or fully implement it, it's up to you.
  The executor should run the whole pipeline or an individual Task if we specify it on the command line as an argument.
 
Transformation
  - You need to extract from the dataset all the recipes that have "beef" as one of the ingredients. Then, we need to add a new field (named       difficulty) calculating the difficulty of the recipe based on:
  - Hard if the total of prepTime and cookTime is greater than 1 hour.
    Medium if the total is between 30 minutes and 1 hour.
    Easy if the total is less than 30 minutes.
    Unknown otherwise.
  - To be able to track changes in the source data over time, add the date of execution as a column to the output. Make sure that historical       data is not overwritten.
  - Store the data in parquet format using the difficulty as partition field.
  
Requirements
 - Write well structured (object oriented), documented and maintainable code.
 - Write unit tests to test the different components of your solution.
 - Make your solution robust against different kind of failures and keep in mind that it should work with bigger data sets.
 - The pipeline should be able to be executed manually from the command line in yarn-client and standalone modes. Add the instructions for      the execution to the pipeline's documentation.
 - The system should handle all kinds of errors and react accordingly, for instance, sending an email with the failure.
 - The system should stop if any of the tasks fails.
 
 
 Prerequites to Execute :
  - JAVA
  - HADOOP 
  - SPARK
  - PYTHON
  
 Below are the Steps to Run in Order:
 - Place all the files including input json file in one location (/tmp )
 - Go to $SPARK_HOME ( Need to Set Spark )
 - execute below command depending on deploy mode ( yarn-client and standalone modes )
 # Run a Python application  YARN cluster
./bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \  # can be client for client mode
  --executor-memory 20G \
  --num-executors 50 \
  tmp/Recipie_Process.py \
  file:/tmp/recipes.json \
  'load,tranform'


# Run a Python application on a Spark standalone cluster
./bin/spark-submit \
  --master spark://207.184.161.138:7077 \
  tmp/Recipie_Process.py \
  file:/tmp/recipes.json \
  'load,transform,query' 'select * from Recipe_table'
