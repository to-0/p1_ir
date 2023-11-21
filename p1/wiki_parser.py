from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import when, col, lower, udf, split, concat_ws, collect_set
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
import sys
from pyspark.sql.functions import lit, explode
import re
import csv

spark = SparkSession.builder.getOrCreate()

def create_schema():
    xml_schema = StructType([
        StructField("title", StringType(), True),
        StructField("revision", StructType([
            StructField("text", StringType(), True)
        ]), True),
        StructField("redirect", StructType([
            StructField("title", StringType(), True)
        ]), True)
    ])
    return xml_schema

def extract_keys_open(column, filename='data.csv'):
    print("Extracting keys")
    #df = spark.read.format('csv').option('delimiter', '\t').option('header', True).load(filename)
    file = open(filename, 'r')
    reader = csv.reader(file, delimiter='\t')
    columns = next(reader)
    keys_set = set()
    for line in reader:
        i = columns.index(column)
        keys = line[i]
        keys = keys.split(';')
        for key in keys:
            if key == '':
                continue
            keys_set.add(key)
    return keys_set


def get_topics(key, raw_text):
    phrases_next_word = [
        "is a field of study in"
        "is a branch of", 
        "is an area of knowledge", 
        "is the study of",
        "is a method of"
    ]   
    phrases_key_is_category = [
        "is a field of study",
        "technology",
        "is a method",
        "is the process of",
        "is a study"
    ]
    # join phrases whe key is a category so we can use them in one simple regex
    phrases_key_is_category = ' | '.join(phrases_key_is_category)
    # do the same for phrases where the second word is a category
    phrases_second_word_is_category = '| '.join(phrases_next_word)
    reg_expr = '({})\s(?:{})'.format(key, phrases_key_is_category)
    reg_expr_second = '(?:{})\s([^\s]*)'.format(phrases_second_word_is_category)
    try:
        matched_group = re.search(reg_expr, raw_text)
        matched_second = re.search(reg_expr_second, raw_text)
    except:
        return ['Not found']
    categories = []
    # we found the category
    if matched_group:
        categories.append(key)
    elif matched_second:
        categories = parse_categories(raw_text)
        if len(categories) > 1:
            categories[0], categories[1] = categories[1], categories[0]
    else:
        categories = parse_categories(raw_text)
    return categories
    
# get categories from the bottom of the wikipage
def parse_categories(raw_text):
    try:
        matches = re.findall(r'\[\[Category:([^]]*)', raw_text)
    except:
        return ['Not found']
    categories = []
    for m in matches:
        m = m.split('|')
        categories.append(''.join(m))
    if len(categories) == 0:
        return ['Not found'] # ''
    return categories


def get_topics_func(merged_keys, dictionary_df):
#return [topic for key in merged_keys.split(';') for topic in dictionary_df.filter(col('key') == key).select('topics').first()[0]]
    topics = []
    keys = merged_keys.split(';')
    for key in keys:
        if key in dictionary_df and len(dictionary_df.get(key)) > 0:
            topics.append(dictionary_df.get(key)[0])
    print("This is topics", topics)
    return topics

def append_topics(key_topic_dict):
    def inner_f(keys):
        topics = []
        if keys is None:
            return ['None']
        keys = keys.split(';')
        for key in keys:
            if key in key_topic_dict and len(key_topic_dict.get(key)) > 0:
                topics.append(key_topic_dict.get(key)[0])
        return topics
    return udf(inner_f, ArrayType(StringType()))

@udf(StringType())
def get_first_topic(topics):
    if topics is not None and len(topics) > 0:
        return topics[0]
    else:
        return 'Not found'
    
def add_topics_column(dictionary_df, column, filename):
    df1 = spark.read.format('csv').option('delimiter', '\t').option('header', True).load(filename) # 'file:///home/data.csv'
    # key_topics_schema = StructType([
    #     StructField("key", StringType(), True),
    #     StructField("topics", ArrayType(StringType()), True)
    # ])
    # dictionary_df =spark.createDataFrame(list(keys_to_topics.items()), key_topics_schema)

    df_exploded = df1.withColumn("keys", split(col(column), ';'))
    df_exploded = df_exploded.select("*", explode("keys").alias("key"))


    result_df = df_exploded.join(dictionary_df, on="key", how="left_outer")
    #result_df = result_df.withColumn("topic", concat_ws(';',col("topics")[0]).alias("topics"))
    result_df = result_df.withColumn("topic", get_first_topic(col("topics")))
    #result_df = result_df.select('link', 'author', 'content', 'publisher', 'year', 'pages', 'ieee_keys', 'author_keys', 'merged_keys', 'topic')
    result_df = result_df.groupBy('link', 'author', 'title', 'content', 'publisher', 'year', 'pages', 'ieee_keys',
                                   'author_keys', 'merged_keys').agg(concat_ws(";", collect_set("topic")).alias("combined_topics"))

    result_df.write.format('csv').option('sep', '\t').option("header", True).mode('overwrite').save("Kalny_df_join")

def join_data(keys_topics, column, filename):
     df = spark.read.format('csv').option('delimiter', '\t').option('header', True).load(filename) # 'file:///home/data.csv'
     #get_topics = udf(lambda keys: get_topics_func(keys, keys_topics), ArrayType(StringType()))
     #merged_df = df.withColumn("topics", get_topics(col(column), lit(keys_topics)))
     merged_df = df.withColumn("topics", append_topics(keys_topics)(col(column)))
     merged_df = merged_df.withColumn("topics", concat_ws(';', col('topics')))
     merged_df.write.format('csv').option('sep', '\t').mode('overwrite').option("header", True).save("/Kalny")

def merge_dicts(dict1, dict2):
    dict1.update(dict2)
    return dict1
    

def parse_dump(path, column, filename):
    schema = create_schema()
    # specify path as folder instead of path to file to read all the dumps
    df = spark.read.format('com.databricks.spark.xml').option('rowTag','page').schema(schema).load(path)
    print("Getting keys")
    keys = list(extract_keys_open(column, filename))
    #keys = [' '.join(str(x)) for x in original_keys]
    print("Got keys")
    # create key column
    filtered_df = df.withColumn("key",
     when(lower(df["title"]).isin(keys), lower(df["title"]))
     .when(lower(df["redirect.title"]).isin(keys), lower(df["redirect.title"]))
     .otherwise(None))
    print("Before filtering")
    filtered_df = filtered_df.filter(col('key').isNotNull())
    # list of dictionaries
    key_topics_dictionaries = filtered_df.rdd.map(lambda row: (row['key'], row['revision']['text'])).map(lambda tpl: {tpl[0]: get_topics(tpl[0], tpl[1])})
    merged_dict = key_topics_dictionaries.reduce(merge_dicts)
    return merged_dict

def main(column='author_keys', filename='data.csv'):
    print("Arguments are ", sys.argv)
    if len(sys.argv) > 1:
        column = sys.argv[1]
    if len(sys.argv) > 2:
        filename = sys.argv[2]
    path = './'
    if len(sys.argv) >= 4:
        path = sys.argv[3]
    keys_to_topics = parse_dump(path, column, filename)

    key_topics_schema = StructType([
        StructField("key", StringType(), True),
        StructField("topics", ArrayType(StringType()), True)
    ])
    dictionary_df =spark.createDataFrame(list(keys_to_topics.items()), key_topics_schema) # ["key", "topics"]
    add_topics_column(dictionary_df, column, filename)
    
    dictionary_df = dictionary_df.withColumn("topics", concat_ws(";", col('topics')))
    dictionary_df.write.format('csv').option('sep', '\t').option("header", True).mode('overwrite').save("Kalny_exported_key_topics")
    #join_data(keys_to_topics, column, filename)
    

main()



