import lucene
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField, StoredField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader, IndexOptions
from org.apache.lucene.store import NIOFSDirectory, MMapDirectory
from org.apache.lucene.util import Version
from org.apache.lucene.search import IndexSearcher
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
    path = File(indexDir).toPath()
    # use disk index, standard analyzer
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
                # index only keys columns
                if keys[index] == 'ieee_keys' or keys[index] == 'author_keys' or keys[index] == 'merged_keys':
                    val = val.split(';')
                else:
                    # store other fields just dont index them
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
    # use default query parser, default key is ieee_keys, if no other key is provided
    qr = QueryParser('ieee_keys', analyzer).parse(query)
    print(qr)
    # return 10 000 most relevant results (or less)
    return searcher.search(qr, 10000)

def display_results(docs, searcher):
    # go over docs
    for hit in docs.scoreDocs:
        doc = searcher.doc(hit.doc)
        # extract link, title, eee_keys etc.
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
            try:
                query = input("Write your query ")
                results = search_f(query, searcher, analyzer)
                print(results)
                display_results(results, searcher)
            except Exception as e:
                print("Something went wrong")
                print(e)

    

main()