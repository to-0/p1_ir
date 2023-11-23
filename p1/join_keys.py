import pandas as pd
import numpy as np

def merge_keys(author_keys, ieee_keys):
    if author_keys == np.nan:
        author_keys = ''
    if ieee_keys == np.nan:
        ieee_keys = ''
    # Split the keys by ;.
    author_keys = author_keys.split(';')
    ieee_keys = ieee_keys.split(';')
    
    #Convert to lowercase
    author_keys = [key.lower() for key in author_keys]
    ieee_keys = [key.lower() for key in ieee_keys]

    # Merge the two lists, removing duplicates.
    merged_keys = list(set(author_keys + ieee_keys))
    return ';'.join(merged_keys)


def main():
    file = input("Write path to file: ")
    df_new = pd.read_csv(file, delimiter='\t')
    df_new = df_new.fillna('')
    df_new['merged_keys'] = df_new.apply(lambda row: merge_keys(row['author_keys'], row['ieee_keys']), axis=1)
    df_new.to_csv('data_merged_keys.csv', sep='\t')