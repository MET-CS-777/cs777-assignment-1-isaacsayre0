from __future__ import print_function

import os
import sys
import builtins
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *

#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

if __name__ == "__main__":
    #Check for correct Usage
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)

    spark = SparkSession.builder \
        .appName("Assignment-1") \
        .getOrCreate()

    sc = spark.sparkContext

    # Set data file path based on first argument
    testFile = sys.argv[1]
    
    try:
            
        print(f"Reading file: {testFile}")
        
        # Read file directly into an RDD (each element is a raw line of text)
        lines = sc.textFile(testFile)

        #Split lines by comma and used defined function to clean the data
        rows = lines.map(lambda line: line.split(",")) 
        clean_rows = rows.map(correctRows).filter(lambda x: x is not None)
        
        #Task 1

        # Extract (medallion, hack_license) pairs
        med_driver = clean_rows.map(lambda x: (x[0], x[1]))

        # Remove duplicates and count distinct drivers per taxi
        distinct_pairs = med_driver.distinct()
        driver_counts = distinct_pairs.map(lambda x: (x[0], 1)) \
                                    .reduceByKey(lambda a, b: a + b)

        # Get top-10 taxis
        top10 = driver_counts.top(num=10,key=lambda x: x[1])

        # Save Task 1 results 
        sc.parallelize([f"{medallion}: {count}" for medallion, count in top10]) \
            .coalesce(1) \
            .saveAsTextFile(sys.argv[2])

        # Print results
        print("Top 10 taxis with most distinct drivers:")
        for medallion, count in top10:
            print(f"{medallion}: {count}")

        #Task 2
        # Extract (hack_license, (total amount, tripTime_minutes)) pairs
        driver_rawEarnings = clean_rows.map(lambda x: (x[1], (float(x[16]),float(x[4])/60)))

        #Reduce by key to determine total driver earnings
        driver_totals = driver_rawEarnings.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

        #Determine Monery Per Minute Earened by Each Driver
        driver_mpm = driver_totals.map(lambda x: (x[0], builtins.round(x[1][0]/x[1][1], 2)))

        #Get top 10 Earnrers defined as Monery per Minute
        top10 = driver_mpm.top(num=10, key=lambda x: x[1])

        # Save Task 2 results to a text file
        sc.parallelize([f"{driver}: {mpm}" for driver, mpm in top10]) \
            .coalesce(1) \
            .saveAsTextFile(sys.argv[3])

        # Print results
        print("Top 10 drivers by average money per minute:")
        for driver, mpm in top10:
            print(f"{driver}: {mpm}")
        sc.stop()
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
    finally:
        # Clean shutdown
        spark.stop()
        print("Spark session stopped.")