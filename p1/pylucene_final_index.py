import lucene
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField, StoredField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader, IndexOptions
from org.apache.lucene.store import NIOFSDirectory, MMapDirectory
from org.apache.lucene.util import Version
from org.apache.lucene.search import IndexSearcher, BooleanQuery, BooleanClause
from org.apache.lucene.queryparser.classic import QueryParser
from pathlib import Path
from java.io import File
import csv 
import os
MAXROW = -1
def create_index():
    indexDir = input("Index name: ")
    indexDir += '/'
    if os.path.exists(indexDir):
        print("Index in this folder already exists")
        return
    else:
        os.mkdir(indexDir)
    path = File('{}/'.format(indexDir)).toPath()
    indexDir = NIOFSDirectory(path)
    writerConfig = IndexWriterConfig(StandardAnalyzer())
    writer = IndexWriter(indexDir, writerConfig)
    filename = input("File: ")

    with open(filename, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        keys = next(reader)
        counter = 0
        for row in reader:
            doc = Document()
            # iterate over values of row
            for index, val in enumerate(row):
                if keys[index] == 'combined_topics' or keys[index] == 'merged_keys':
                    if keys[index]== 'combined_topics':
                        print(val)
                    val = val.split(';')
                else:
                    # store other fields just dont index them 
                    # todo Try StoreField
                    # fieldType = FieldType()
                    # fieldType.setStored(True)
                    # fieldType.setIndexOptions(IndexOptions.NONE)
                    # field = Field(keys[index], val, fieldType)
                    field = StoredField(keys[index], val)
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

def search_f(query, searcher, analyzer):
    topics = query.split(',')
    searched_topics = []
    for topic in topics:
        topic = topic.split(":")
        searched_topics.append(analyzer.normalize('combined_topics', ' '.join(topic[1:])).utf8ToString())
    # topics = topics[1].split(':')
    # topics = [analyzer.normalize('combined_topics', x).utf8ToString() for x in topics]
    #qr = QueryParser('ieee_keys', analyzer).parse(query)
    mainQuery = BooleanQuery.Builder()
    que = BooleanQuery.Builder()#.Builder()
    for topic in searched_topics:
        clause = BooleanClause(QueryParser("combined_topics", analyzer).parse(topic), BooleanClause.Occur.SHOULD)
        que.add(clause)
    que = que.build().toString()
    mainQuery.add(BooleanClause(QueryParser("combined_topics",analyzer).parse(que), BooleanClause.Occur.MUST))
    mainQuery = mainQuery.build()
    #print(qr)
    print(mainQuery)
    topic_frequencies = {}
    hits = searcher.search(mainQuery, 1000)
    print("Tu")
    # go over hits
    for hit in hits.scoreDocs:
        doc_id = hit.doc
        doc = searcher.doc(doc_id)
        keys = doc.getFields('combined_topics')
        # go over topics in document
        for key in keys:
            # is topic in our topic list?
            key = key.stringValue()
            key = analyzer.normalize('combined_topics', key).utf8ToString()
            if key in searched_topics:
                # process
                if key in topic_frequencies:
                    topic_frequencies[key]['count'] += 1
                    topic_frequencies[key]['docs'].append(doc)
                else:
                    topic_frequencies[key] = {}
                    topic_frequencies[key]['count'] = 1
                    topic_frequencies[key]['docs'] = []
                    topic_frequencies[key]['docs'].append(doc)
    sorted_dict = dict(sorted(topic_frequencies.items(), key=lambda item: item[1]['count'], reverse=True))
    return sorted_dict
    
        


def display_results(sorted_dict):
    for key, value in sorted_dict.items():
        for doc in value.get('docs'):
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
            print(res_keys)
            print(res_topics, res_authors)
            print("==="*50)
    print(list(sorted_dict.keys()))


def print_statistics(results):
    for key, item in results.items():
        print(f'Topic:{key}, count:{item['count']}')
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
        while True:
            print("Search for document, possible fields are ieee_keys author_keys. Use syntax key:value key2:value2")
            # try:
            query = input("Write your query ")
            results = search_f(query, searcher, analyzer)
            display_results(results)
            print_statistics(results)
            
            # tu
            # except Exception as e:
            #     print("Something went wrong")
            #     print(e)

main()