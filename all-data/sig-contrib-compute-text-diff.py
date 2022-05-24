#!/usr/bin/python3
import dask
import dask.dataframe as ddf
import os
from glob import glob
import re
import pandas as pd
import numpy as np
import collections
from multiprocessing import Pool
from functools import partial
import time
import ast
import argparse

argparser= argparse.ArgumentParser('processed the revision text')
argparser.add_argument('infile')
args=argparser.parse_args()
folder = args.infile + '/'

directory = '/home/ubuntu/intermediate-result/all-data/added-removed-without-filters/' + folder
df = pd.read_parquet(directory)
def string_to_counter(s):
    s = ast.literal_eval(s) 
    return collections.Counter(s)
def find_diffs(d):
    # not care about anon editors
    if pd.isnull(d['contributor.username']):
        return None,None
    parent_id = d['revision.parentid']
    current = string_to_counter(d['processed_text'])
    if parent_id == '':
        added = current
        removed = collections.Counter()
    elif parent_id in df.index:
        parent = string_to_counter(df.loc[parent_id]['processed_text'])
        added = current - parent
        removed = parent - current
    else:
        # cannot find its parent id
        return None,None
    return added,removed
def parallelize(data, func, num_of_processes=16):
    data_split = np.array_split(data, num_of_processes)
    pool = Pool(num_of_processes)
    data = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data

def run_on_subset(func, data_subset):
    return data_subset.apply(func, axis=1, result_type = 'expand')

def parallelize_on_rows(data, func, num_of_processes=16):
    return parallelize(data, partial(run_on_subset, func), num_of_processes)

df[['added','removed']] = parallelize_on_rows(df, find_diffs)
x = df[df['added'].notnull()]
x = x[x['added']!={}]
y = x[['added']]
def get_added_words_number(collection):
    return sum(collection.values())

y['added_len'] = y['added'].apply(get_added_words_number)
y = y[['added_len']]
y.to_parquet('/home/ubuntu/intermediate-result/all-data/added_token_len/' + folder[:-1])

