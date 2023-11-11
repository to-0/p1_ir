import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import os
import csv
from pyspark.sql.functions import lit
import re

spark = SparkSession.builder.getOrCreate()


columns = ['ieee_keys', 'author_keys']

keys_topics = {}
# extracts keys from my .csv file containing info from ieee that I have crawled out
def extract_keys(column, filename='test.csv'):
    print("Extracting keys")
    df = spark.read.format('csv').option('delimiter', '\t').option('header', True).load(filename)
    keys = set()
   
    print("Selected column")
    filtered = df.filter(col(column).isNotNull())
    cl = filtered.select(column).rdd
    print("RDD")
    keys = cl.map(lambda row: row[0].split(';')).map(lambda values: set(values)).reduce(lambda set1, set2: set1.union(set2))
    #keys = filtered.flatMap(lambda row: row[0].split(';')).map(lambda values: set(''.join(values))).reduce(lambda set1, set2: set1.union(set2))
    print("Returning keys")
    return keys


# it would be better to read the files in a distributed way
#   for each file find all of the keys
#   create categories based
def get_topics(key, raw_text):
    phrases_next_word = [
        "is a field of study in"
        "is a branch of", 
        "is the process of",
        "is an area of knowledge", 
        "is the study of",
        "is a method of"
    ]   
    phrases_key_is_category = [
        "is a field of study",
        "technology",
        "is a method",
        "is a study"
    ]
    # join phrases whe key is a category so we can use them in one simple regex
    phrases_key_is_category = ' | '.join(phrases_key_is_category)
    # do the same for phrases where the second word is a category
    phrases_second_word_is_category = '| '.join(phrases_next_word)
    reg_expr = '({})\s(?:{})'.format(key, phrases_key_is_category)
    reg_expr_second = '(?:{})\s([^\s]*)'.format(phrases_second_word_is_category)

    matched_group = re.search(reg_expr, raw_text)
    matched_second = re.search(reg_expr_second, raw_text)
    categories = []
    # we found the category
    if matched_group:
        print("Matched  first regex")
        categories.append(key)
    elif matched_second:
        print("Matched second  regex ")
        categories = parse_categories(raw_text)
        if len(categories) > 1:
            categories[0], categories[1] = categories[1], categories[0]
    else:
        categories = parse_categories(raw_text)
    return categories
    
# get categories from the bottom of the wikipage
def parse_categories(raw_text):
    matches = re.findall(r'\[\[Category:([^]]*)', raw_text)
    categories = []
    for m in matches:
        m = m.split('|')
        categories.append(''.join(m))
    return categories

def create_new_column(row):
    row['topics'] = []
    row['author_keys'] = row['author_keys'].split()
    for key in row['author_keys']:
        key = str(key)
        if key in keys_topics and len(key_topics.get(key)) > 0:
            row['topics'].append(keys_topics.get(key)[0]) # add only the first topic
    return row

def create_topics(column_keys, key_topics):
    temp = column_keys.split(';')
    topics = []
    for key in temp:
        key = str(key)
        if key in key_topics and len(key_topics.get(key)) >= 2:
            topics.append(key_topics.get(key)[0:2]) # add only the 2 topics
        elif key in key_topics and len(key_topics.get(key)) == 1:
            topics.append(key_topics.get(key)[0])


def join_new_data_parallel(key_topics):
    df = spark.read.format('csv').option('delimiter', '\t').option('header', True).load('data.csv')
    df.withColumn('topics', lit(create_topics(df['author_keys'], key_topics)))
    new_df = df.alias
    return new_df

def join_new_data(key_topics, column):
    f = open('home/data.csv','r')
    reader = csv.reader(f, delimiter='\t')
    f2 = open('home/merged.csv','w+')
    writer = csv.writer(f2, delimiter='\t')
    #df = spark.read.format('csv').option('delimiter', '\t').option('header', True).load('test.csv')
    columns = next(reader)
    counter = 1
    writer.writerow(columns)

    for line in reader:
        if column == 'author_keys':
            column_index = -1
        else:
            column_index = -2
        
        author_keys = line[-1]
        if author_keys is None or author_keys == '':
            row = line
            row.append([])
            writer.writerow(row)
            counter += 1
            continue
        author_keys = author_keys.split(';')
        topics_column = []
        # iterate over author keys
        for key in author_keys:
            # if we have found corresponding topics (categories) to that key get the first one, which is usually the most relevant
            if key in key_topics and len(key_topics[key]) > 0:
                topics = key_topics.get(key)
                topics_column.append(topics[0])
        row = line
        row.append(topics_column)
        writer.writerow(row)
        counter += 1
    f.close()
    f2.close()


    

def parse_dump(path, column, filename):
    # specify path as folder instead of path to file to read all the dumps
    df = spark.read.format('com.databricks.spark.xml').option('rowTag','page').load(path)
    titles = df.select('title')
    print("Getting keys")
    keys = extract_keys(column, filename)
    #keys = [' '.join(str(x)) for x in original_keys]
    print("Got keys")
    # create key column
    filtered_df = df.withColumn("key",
     when(df["title"].isin(keys), df["title"])
     .when(df["redirect._title"].isin(keys), df["redirect._title"])
     .otherwise(None))
    print("Before filtering")
    filtered_df = filtered_df.filter(col('key').isNotNull())
    # df.filter(col('title').isin(keys) | col('redirect._title').isin(keys)) # filter by titles or redirects
    # regex \[\[Category:([^]]*)
    key_topics_dictionaries = filtered_df.rdd.map(lambda row: (str(row['key']), str(row['revision']['text']))).map(lambda tpl: {tpl[0]: get_topics(tpl[0], tpl[1])}).collect()
    result_dict = {}
    for d in key_topics_dictionaries:
        result_dict.update(d)
    return result_dict

def main(column='author_keys', filename='data.csv'):
    path = './'
    keys_to_topics = parse_dump(path, column, filename)
    f = open('exported_keys_to_topics.txt', 'w')
    for key, value in keys_to_topics.items():
        f.write('{}:{}'.format(key, value))
        f.write('\n')
    join_new_data(keys_to_topics, column)