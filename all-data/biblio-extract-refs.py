#!/usr/bin/python3

import dask
import dask.dataframe as ddf
import os
from glob import glob
import re
import pandas as pd
import numpy as np
import time
import mwparserfromhell
import argparse
from multiprocessing import  Pool
from functools import partial


argparser= argparse.ArgumentParser('extract refs for selected article and selected editor and their parents revs')
argparser.add_argument('infile')
args=argparser.parse_args()

original_folder = args.infile

folder = original_folder + '/'
path = '/home/ubuntu/enwiki-columns/'


all_folders = glob(path + '*')

selected_editors = pd.read_parquet('/home/ubuntu/xinrui/selected-editors-based-on-cc').index
kept_users = list(selected_editors)
articles = pd.read_parquet('/home/ubuntu/xinrui/process-all-wiki/c-score/result/selected_articles_based_on_c_score.parquet').index
kept_titles = list(articles)


page_titles = [f for f in all_folders if 'page.title' in f]
titles = ddf.read_parquet(page_titles).compute()
titles = titles.drop(columns={'dir0'})

titles = titles[titles['page.title'].isin(kept_titles)]

editors_files = [f for f in all_folders if 'contributor.username' in f]
editors = ddf.read_parquet(editors_files).compute()
editor = editors.drop(columns={'dir0'})
kept_editor_df = editor[editor['contributor.username'].isin(kept_users)]

parent_ids_files = [f for f in all_folders if 'revision.parentid' in f]
parent_ids = ddf.read_parquet(parent_ids_files).compute()
parent_ids = parent_ids.drop(columns={'dir0'})


with_parent_id = kept_editor_df.join(parent_ids)
kept_index = list(set(list(with_parent_id.index) + list(with_parent_id['revision.parentid'])))

df = editor.join(titles)
df = df[df.index.isin(kept_index)]


text_files = [f for f in all_folders if 'revision.text' in f]
texts = ddf.read_parquet(text_files).compute()
text = texts.drop(columns={'dir0'})
df = df.join(text)


def get_rev(d):
    text = d['revision.text']
    parsed_code = mwparserfromhell.parse(text)
    refs = []
    for tag in parsed_code.filter_tags():
        if tag.tag == 'ref':
            refs.append(tag.strip())
    return '\t'.join(refs)

def parallelize(data, func, num_of_processes=16):
    data_split = np.array_split(data, num_of_processes)
    pool = Pool(num_of_processes)
    data = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data

def run_on_subset(func, data_subset):
    return data_subset.apply(func, axis=1)

def parallelize_on_rows(data, func, num_of_processes=16):
    return parallelize(data, partial(run_on_subset, func), num_of_processes)

df['refs'] = parallelize_on_rows(df, get_rev)
res = df[['refs']]

res_df = ddf.from_pandas(res,chunksize=10000)
res_df.to_parquet('/home/ubuntu/intermediate-result-new-select-editors/bibliography/refs-all-data/' + folder)
