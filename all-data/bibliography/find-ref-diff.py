#!/usr/bin/python3


import dask
import dask.dataframe as ddf
import os
from glob import glob
import re
import pandas as pd
import numpy as np
import collections
import argparse

argparser= argparse.ArgumentParser('find added and removed refs for selected editors and articles')
argparser.add_argument('infile')
args=argparser.parse_args()

original_folder = args.infile

selected_articles = pd.read_parquet('/home/ubuntu/xinrui/process-all-wiki/c-score/result/selected_articles_based_on_c_score.parquet').index
folder = original_folder + '/'
path = '/home/ubuntu/enwiki-columns/' + folder 


all_folders = glob(path + '*')
    

# page_title = dd.read_parquet(path + '*title*').drop(columns={'dir0'}).compute()
page_title_files = [s for s in all_folders if 'page.title' in s]
titles = [ddf.read_parquet(file) for file in page_title_files]
page_title = ddf.concat(titles)
page_title = page_title.compute()


ref_path = '/home/ubuntu/intermediate-result-new-select-editors/bibliography/refs-all-data/'
refs = ddf.read_parquet(ref_path + folder).compute()

df = refs.join(page_title)
df = df[df['page.title'].isin(selected_articles)]


parents_files = [s for s in all_folders if 'revision.parentid' in s]
parents = [ddf.read_parquet(file) for file in parents_files]
parent_ids = ddf.concat(parents)
parent_ids = parent_ids.compute()


df = df.join(parent_ids)

selected_editors = pd.read_parquet('/home/ubuntu/xinrui/selected-editors-based-on-cc').index
kept_users = list(selected_editors)

editors_files = [f for f in all_folders if 'contributor.username' in f]
editor = ddf.read_parquet(editors_files).compute()
editor = editor.drop(columns={'dir0'})


df = df.join(editor)


def find_added_removed(d):
    curr_id = d.name
    # parents that are used to compared, not need to compute diff for this
    if d['contributor.username'] not in kept_users:
        return pd.Series([None,None],index=['refs.added','refs.removed'])
    # else compute difference
    prev_id = d['revision.parentid']
    curr_content = collections.Counter(df.loc[curr_id]['refs'].split('\t'))
    prev_content = collections.Counter()
    
    if prev_id in df.index:
        prev_content = collections.Counter(df.loc[prev_id]['refs'].split('\t'))
    
    added = curr_content - prev_content
    removed = prev_content - curr_content
    
    return pd.Series([added,removed],index=['refs.added','refs.removed'])


df[['refs.added','refs.removed']] = df.apply(find_added_removed,axis=1)
df = df[(df['refs.added']!={}) & (df['refs.removed']!={})]

df = df[df['contributor.username'].isin(kept_users)]
res = df[['refs.added','refs.removed']]
res_df = ddf.from_pandas(res,chunksize=10000)
res_df.to_parquet('/home/ubuntu/intermediate-result-new-select-editors/bibliography/refs-added-removed-info/' + folder, object_encoding='json')


