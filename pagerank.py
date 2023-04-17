#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, lower, lit, count, sum
import pandas as pd
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'

def PageRanks(input_path, output):
    spark = SparkSession.builder.getOrCreate()
    
    df = spark.read.option("delimiter", "\t").csv(input_path)
    
    df = df.withColumnRenamed('_c0', 'title').withColumnRenamed('_c1','internal_link')
        
    df_node = df.groupby('title').agg(count(lit(1)).alias('n_neighbor')).withColumn('rank', lit(1))

    for i in range(10):
        
        df_update = df.join(df_node, on = 'title').withColumn('contribution_to_inner', 
                        col('rank') / col('n_neighbor')) \
            .fillna(0, subset="contribution_to_inner") \
            .groupby('internal_link') \
            .agg(sum('contribution_to_inner') \
            .alias('total_contribution')) \
            .withColumnRenamed('internal_link', 'title')

        
        # combine the updated nodes to the current nodes
        df_node = df_node.join(df_update, how="leftouter", on="title").fillna(0, subset="total_contribution").rdd.map(lambda x:(x[0], x[1], 0.15 + 0.85 * x[3])).toDF(['title', 'n_neighbor', 'rank'])
        
    result = df_node.select('title', 'rank')
    
    result_top = result.sort(['title', 'rank'],ascending=True).limit(5)
    result_top.toPandas().to_csv(output_path , sep='\t', index=False, header=False)
    
    # result.coalesce(1).write.option("delimiter", "\t").mode("overwrite").csv(output_path)
    


# use enwiki_small.xml as the input file for Task 2
PageRanks(input_path = 'gs://hw2_personal_bucket/small_internal_link/part-00000-35d3135e-fcf5-43a7-8fc9-95ce622f4b55-c000.csv', 
          output_path = 'gs://hw2_personal_bucket/p1t3.csv')




