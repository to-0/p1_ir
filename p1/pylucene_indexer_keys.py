import lucene
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader, IndexOptions
from org.apache.lucene.store import NIOFSDirectory, MMapDirectory
from org.apache.lucene.util import Version
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import QueryParser
from pathlib import Path
from java.io import File
import csv 
import os
MAXROW = 100
def create_index(indexDir):
    path = File('my_index/').toPath()
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
                if keys[index] == 'ieee_keys' or keys[index] == 'author_keys':
                    val = val.split(';')
                else:
                    # store other fields just dont index them
                    title_field = FieldType()
                    title_field.setStored(True)
                    title_field.setIndexOptions(IndexOptions.NONE)
                    field = Field(keys[index], val,TextField.TYPE_STORED)
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

def search_f(query, searcher, analyzer):
    qr = QueryParser('title', analyzer).parse(query)
    print(qr)
    return searcher.search(qr, 10000)

def display_results(docs, searcher):
    for hit in docs.scoreDocs:
        doc = searcher.doc(hit.doc)
        link = doc.getField("link").stringValue()
        title = doc.getField('title').stringValue()
        res_keys = []
        res_authors = []
        keys = doc.getFields('ieee_keys')
        for key in keys:
            val =  key.stringValue()
            res_keys.append(key.stringValue())
        
        authors = doc.getFields('author')
        for author in authors:
            res_authors.append(author.stringValue())
        print("Title:", title)
        print("Link:", link)
        print(res_keys, res_authors)
        print("==="*50)


def main():
    lucene.initVM()
    index_dir = 'myind'
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)

    option = str(input("Create new index? y/n")).lower()

    if option == "y":
        create_index(index_dir)
    
    search = str(input("Search y/n:")).lower()
    if search == "y":
        indexDir = NIOFSDirectory(File('my_index/').toPath())
        searcher = IndexSearcher(DirectoryReader.open(indexDir))
        analyzer = StandardAnalyzer()
        while True:
            print("Search for document, possible fields are link	title	author	content	publisher	year	pages	ieee_keys	author_keys. Use syntax key:value key2:value2")
            try:
                query = input("Write your query ")
                results = search_f(query, searcher, analyzer)
                print(results)
                display_results(results, searcher)
            except Exception as e:
                print("Something went wrong")
                print(e)

    

            