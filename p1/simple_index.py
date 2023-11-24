import nltk
import csv
from collections import OrderedDict
documents_list = []
MAX = -1
def index_documents(reader):
    inverted_indices = OrderedDict()
    keys = next(reader)
    for key in keys:
        inverted_indices[key] = {} # single index for column
    i = 0
    while True: # i is the doc id
        if MAX != -1 and i >= MAX:
            break
        try:
            doc = next(reader)
        except StopIteration:
            break
        documents_list.append(doc)
        for column_i in range(len(doc)): # iterate over overy column, index is also key index
            if keys[column_i] != 'ieee_keys' and keys[column_i] != 'author_keys' and keys[column_i] != 'merged_keys':
                continue
            else:
                terms = doc[column_i].split(';')
                # if keys[column_i] == 'merged_keys':
                #     print('ahoj')
            for term in terms:
                if term == '':
                    continue
                stemmed_term = term.lower()
                # if term is encountered for the first time
                if stemmed_term not in inverted_indices[keys[column_i]]:
                    ordered = OrderedDict()
                    ordered[i] = 1
                    inverted_indices[keys[column_i]][stemmed_term] = {}
                    inverted_indices[keys[column_i]][stemmed_term]['doc_freq'] = ordered
                    inverted_indices[keys[column_i]][stemmed_term]['freq'] = 1
                else:
                    # check if we have this document saved already or not
                    if i not in inverted_indices[keys[column_i]][stemmed_term]['doc_freq']:
                        # ordered = OrderedDict()
                        # ordered[i] = 1
                        # save document id and frequency
                        inverted_indices[keys[column_i]][stemmed_term]['doc_freq'][i] = 1
                    else:
                        inverted_indices[keys[column_i]][stemmed_term]['doc_freq'][i] += 1 # increment frequency by one 
                    inverted_indices[keys[column_i]][stemmed_term]['freq'] += 1
        i += 1
    sorted_indices = {}
    # sort all indices for each column
    for key in keys:
        # get terms
        index_terms = list(inverted_indices[key].keys())
        # sort them
        index_terms.sort()
        sorted_dict = {i: inverted_indices[key][i] for i in index_terms}
        sorted_indices[key] = sorted_dict
    return sorted_indices


def intersect_two(p1, p2):
    result = []
    # get posting lists
    # p1 = index[term1]['doc_freq'] # ordered dictionary doc: frequency
    # p2 = index[term2]['doc_freq']
    # doc_list1 = list(p1.keys())
    # doc_list2 = list(p2.keys())
    doc_list1 = p1
    doc_list2 = p2
    i = 0
    j = 0
    while i < len(doc_list1) and j < len(doc_list2):
        if doc_list1[i] == doc_list2[j]:
            result.append(doc_list1[i])
            i += 1
            j += 1
        elif doc_list1[i] < doc_list2[j]:
            i += 1
        else:
            j += 1
    return result
                   
def get_terms_postings_ordered_by_frequency(indices, searched_terms, searched_columns):
    terms = {}
    for i, term in enumerate(searched_terms):
        index = indices[searched_columns[i]]
        posting_list = index[term] # ordered dictionary doc: freq
        terms[term] = posting_list['freq']
    sorted_d = dict(sorted(terms.items(), key=lambda item: item[1]))
    return sorted_d



def n_intersect(indices, searched_terms, searched_columns):
    sorted_terms_by_freq_dict = get_terms_postings_ordered_by_frequency(indices, searched_terms, searched_columns)
    sorted_terms_by_freq = list(sorted_terms_by_freq_dict.keys())
    i = 0
    result = sorted_terms_by_freq[0]
    result = list(indices[searched_columns[0]][result]['doc_freq'].keys())
    i = 1
    while i < len(sorted_terms_by_freq):
        index = indices[searched_columns[i]]
        result = intersect_two(result, list(index[sorted_terms_by_freq[i]]['doc_freq'].keys()))
        i += 1
    return result
    


def search_index(indices, query):
    query = query.lower()
    query = query.split(',')
    pairs = len(query)
    # search only single term in specific column
    if pairs == 1:
        query = ''.join(query).split(':')
        term = query[1]
        column_name = query[0]
        query = term.lower()
        if term in indices[column_name]:
            term_postings = get_terms_postings_ordered_by_frequency(indices, [term], [column_name])
            return list(term_postings.values())
        else:
            return "Term does not exist in index"
    elif pairs == 2:
        key_val1 = query[0].split(':')
        key_val2 = query[1].split(':')
        # get correct indices for each val
        index1 = indices[key_val1[0]]
        index2 = indices[key_val2[0]]

        # get posting lists
        p1 = list(index1[key_val1[1]]['doc_freq'].keys())
        p2 = list(index2[key_val2[1]]['doc_freq'].keys())
        return intersect_two(p1, p2)
    else:
        searched_terms = []
        searched_columns = []
        for pair in query:
            pair = pair.split(':')
            if len(pair) != 2:
                print("Wrong query format")
                return
            searched_terms.append(pair[1])
            searched_columns.append(pair[0])
        return n_intersect(indices, searched_terms, searched_columns)


def main():
    path = input("File path: ")
    f = open(path, 'r', encoding='utf-8')
    reader = csv.reader(f, delimiter='\t')
    index = index_documents(reader)
    # print(list(index['ieee_keys'].keys()))
    while True:
        query = input("Write searched term in form of key:value when using multiple values u can use key1:value1,key2:value2 ")
        doc_ids = search_index(index, query)
        print(doc_ids)
        if doc_ids == 'Term does not exist in index':
            continue
        for doc_id in doc_ids:
            print(documents_list[doc_id])
            print("==="*20)

main()