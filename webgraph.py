#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_test.xml')
df = df.filter(df.revision.text._VALUE.isNotNull())

from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, explode, lower
import numpy as np
import regex

def find_link(text):
    
    links_final = []
    
    pattern = '\[\[((?:[^[\]]+|(?R))*+)\]\]'
    matches = regex.findall(pattern, text)

    for m in matches:
        links = m.split('|')
        
        for link in links:
            if link.find(':') >= 0:  # contains ':'
                if not link.startswith('Category:'):  # not startswith 'Category:'
                    continue
                else:  # startswith 'Category:'
                    links_final.append(link.lower())
                    break
            elif link.find('#') >= 0:  # contains '#'
                continue
            else:
                links_final.append(link.lower())
                break
                
    return links_final



find_link_udf = udf(lambda text: find_link(text), ArrayType(StringType()))
title_links = df.select(lower(df.title), explode(find_link_udf(df.revision.text._VALUE)).alias("links")).sort("title", "links")
title_links.coalesce(1).write.option("sep", "\t").mode("overwrite").csv("gs://cseehw/t2q2.csv")