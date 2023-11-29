import lucene
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField, StoredField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader, IndexOptions, Term
from org.apache.lucene.store import NIOFSDirectory, MMapDirectory
from org.apache.lucene.util import Version
from org.apache.lucene.search import IndexSearcher, BooleanQuery, BooleanClause, TermQuery
from org.apache.lucene.queryparser.classic import QueryParser
from pathlib import Path
from java.io import File
import csv 
import os
MAXROW = -1
SEARCH_N = 2000
def create_index():
    indexDir = input("Index name: ")
    indexDir += '/'
    if os.path.exists(indexDir):
        print("Index in this folder already exists")
        return
    else:
        os.mkdir(indexDir)
    path = File(indexDir).toPath()
    indexDir = NIOFSDirectory(path)
    writerConfig = IndexWriterConfig(StandardAnalyzer())
    writer = IndexWriter(indexDir, writerConfig)
    filename = input("File: ")

    with open(filename, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        keys = next(reader)
        print("These are keys", keys)
        counter = 0
        for row in reader:
            doc = Document()
            # iterate over values of row
            for index, val in enumerate(row):
                if keys[index] == 'ieee_keys' or keys[index] == 'author_keys' or keys[index] == 'merged_keys' or keys[index] == 'combined_topics':
                    val = val.split(';')
                else:
                    # index also other fields
                    field = Field(keys[index], val, TextField.TYPE_STORED)
                    doc.add(field)
                    continue
                # iterate over words in a column
                for word in val:
                    field = Field(keys[index], word ,TextField.TYPE_STORED)
                    doc.add(field)
            if MAXROW != -1 and counter >= MAXROW:
                print("Limit exceeded")
                break
            counter += 1
            writer.addDocument(doc)
    writer.close()
    return indexDir

def search_advanced(query, searcher, analyzer):
    topics = query.split(',')
    searched_topics = []
    columns = set()
    # topics = topics[1].split(':')
    # topics = [analyzer.normalize('combined_topics', x).utf8ToString() for x in topics]
    #qr = QueryParser('ieee_keys', analyzer).parse(query)
    mainQuery = BooleanQuery.Builder()
    que = BooleanQuery.Builder()#.Builder()
    for topic in topics:
        topic = topic.split(":")
        if topic[1][-1] ==' ':
            topic[1][-1] = ''
        #clause = BooleanClause(QueryParser('combined_topics', analyzer).parse(topic), BooleanClause.Occur.SHOULD)
        clause = BooleanClause(TermQuery(Term(topic[0], topic[1])), BooleanClause.Occur.SHOULD)
        columns.add(topic[0])
        searched_topics.append(analyzer.normalize('combined_topics', ' '.join(topic[1:])).utf8ToString())
        que.add(clause)

    # for topic in topics:
    #     clause = BooleanClause(QueryParser('combined_topics', analyzer).parse(topic), BooleanClause.Occur.SHOULD)
    #     topic = topic.split(':')
    #     columns.add(topic[0])
    #     searched_topics.append(analyzer.normalize('combined_topics', ' '.join(topic[1:])).utf8ToString())
    #     que.add(clause)
    que = que.build().toString()
    mainQuery.add(BooleanClause(QueryParser('combined_topics',analyzer).parse(que), BooleanClause.Occur.MUST))
    mainQuery = mainQuery.build()
    #print(qr)
    print(mainQuery)
    topic_frequencies = {}
    hits = searcher.search(mainQuery, SEARCH_N)
    # go over hits&
    for hit in hits.scoreDocs:
        doc_id = hit.doc
        doc = searcher.doc(doc_id)
        # iterate over columns used in query
        for column in columns:
            keys = doc.getFields(column)
            # go over topics in document
            for key in keys: # 
                # is topic in our topic list?
                key = key.stringValue()
                key = analyzer.normalize('combined_topics', key).utf8ToString()
                if key[-1] == ' ':
                    key = key[:-1]
                
                if key in searched_topics:
                    # process
                    if key in topic_frequencies:
                        if doc not in topic_frequencies[key]['docs']: # to avoid duplicates
                            topic_frequencies[key]['count'] += 1
                            topic_frequencies[key]['docs'].append(doc)
                    else:
                        topic_frequencies[key] = {}
                        topic_frequencies[key]['count'] = 1
                        topic_frequencies[key]['docs'] = []
                        topic_frequencies[key]['docs'].append(doc)

                # for searched_topic in searched_topics:
                #     # in case if the key is something like 'alias fdsafs' and our searched topic is alies, this will match, keys can be longer
                #     if searched_topic in key:
                #         # process
                #         if searched_topic in topic_frequencies:
                #             if doc not in topic_frequencies[searched_topic]['docs']: # to avoid duplicates
                #                 topic_frequencies[searched_topic]['count'] += 1
                #                 topic_frequencies[searched_topic]['docs'].append(doc)
                #         else:
                #             topic_frequencies[searched_topic] = {}
                #             topic_frequencies[searched_topic]['count'] = 1
                #             topic_frequencies[searched_topic]['docs'] = []
                #             topic_frequencies[searched_topic]['docs'].append(doc)
    sorted_dict = dict(sorted(topic_frequencies.items(), key=lambda item: item[1]['count'], reverse=True))
    return hits.scoreDocs, sorted_dict


def search_f(query, searcher, analyzer):
    if ',' in query:
        return search_advanced(query, searcher, analyzer)
    
    qr = QueryParser('combined_topics', analyzer).parse(query)
    print(qr)
    hits = searcher.search(qr, SEARCH_N)
    frequencies = {}
    comparison = False
    for hit in hits.scoreDocs:
        doc_id = hit.doc
        doc = searcher.doc(doc_id)
        pairs = query.split(' OR ')
        for pair in pairs:
            if pair in frequencies:
                if doc not in frequencies[pair]['docs']: # to avoid duplicates
                    frequencies[pair]['count'] += 1
                    frequencies[pair]['docs'].append(doc)
            else:
                frequencies[pair] = {}
                frequencies[pair]['count'] = 1
                frequencies[pair]['docs'] = []
                frequencies[pair]['docs'].append(doc)
    sorted_dict = dict(sorted(frequencies.items(), key=lambda item: item[1]['count'], reverse=True))
    return (hits.scoreDocs, sorted_dict)

def display_results(sorted_dict):
    displayed_topics = set()
    for key, value in sorted_dict.items():
        for doc in value.get('docs'):
            if doc in displayed_topics:
                continue
            link = doc.getField("link").stringValue()
            title = doc.getField('title').stringValue()
            res_keys = []
            res_authors = []
            res_topics = []
            topics = doc.getFields('combined_topics')
            keys = doc.getFields('merged_keys')
            for key in keys:
                res_keys.append(key.stringValue())
            for key in topics:
                res_topics.append(key.stringValue())
            
            authors = doc.getFields('author')
            for author in authors:
                res_authors.append(author.stringValue())
            print("Title:", title)
            print("Link:", link)
            print("Merged keys", res_keys)
            print("Topics:", res_topics)
            print("Authors:", res_authors)
            print("==="*50)
            displayed_topics.add(doc)

def display_basic(hits, searcher):
    for hit in hits:
        doc = searcher.doc(hit.doc)
        link = doc.getField("link").stringValue()
        title = doc.getField('title').stringValue()
        res_keys = []
        res_authors = []
        res_topics = []
        topics = doc.getFields('combined_topics')
        keys = doc.getFields('merged_keys')
        for key in keys:
            res_keys.append(key.stringValue())
        for key in topics:
            res_topics.append(key.stringValue())
        
        authors = doc.getFields('author')
        for author in authors:
            res_authors.append(author.stringValue())
        print("Title:", title)
        print("Link:", link)
        print("Merged keys", res_keys)
        print("Topics:", res_topics)
        print("Authors:", res_authors)
        print("==="*50)

def print_statistics(results):
    for key, item in results.items():
        print(f'Topic:{key}, count:{item["count"]}')

def unit_tests(searcher, analyzer):
    input_queries = ['author:"Wei Xu"', 'combined_topics:voltage, combined_topics:voltage regulation', 'merged_keys:"data science" AND publisher:IEEE AND content:python', 
                     'combined_topics:topology AND title:"analysis swarm"', 'combined_topics:wire, combined_topics:data security, combined_topics:quality control',
                     'combined_topics:"Machine learning" AND combined_topics:privacy AND title:federated']
    for query in input_queries:
        print(query)
        hits, frequencies = search_f(query, searcher, analyzer)
        if ',' in query and '"' not in query:
            display_results(frequencies)
            print_statistics(frequencies)
        else:
            display_basic(hits, searcher)
        temp = input("Press enter to continue on next query:")


def main():
    lucene.initVM()
    option = str(input("Create new index? y/n")).lower()
    index_dir = ''
    if option == "y":
        index_dir = create_index()
    search = str(input("Search y/n:")).lower()
    if search == "y":
        if index_dir == '':
            index_dir = input("Index directory? ")
            index_dir = NIOFSDirectory(File(index_dir).toPath())
        searcher = IndexSearcher(DirectoryReader.open(index_dir))
        analyzer = StandardAnalyzer()
        tests = input("Do you want to run unit tests? y/n")
        if tests == 'y':
            unit_tests(searcher, analyzer)
        while True:
            print("Search for document. You can use AND OR operators, if you use , between fields it will be treated as advanced search where you search for one or the other and then want to compare them based on frequencies")
            try:
                query = input("Write your query ")
                search_opt  = input("Basic search or term based search (statistics included)? 1/2")
                if search_opt == '1':
                    hits, frequencies = search_f(query, searcher, analyzer)
                    print('*'*100)
                else:
                    hits, frequencies = search_advanced(query, searcher, analyzer)
                    print('*'*100)
                
                if ',' in query:
                    #print(frequencies)
                    display_results(frequencies)
                    print_statistics(frequencies)
                else:
                    option = input('basic display 1 or statistics display 2? ')
                    if option == '1':
                        display_basic(hits, searcher)
                    else:
                        display_results(frequencies)
                        print_statistics(frequencies)
            except Exception as e:
                print("Something went wrong")
                print(e)

    

main()