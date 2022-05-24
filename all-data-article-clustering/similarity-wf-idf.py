#!/usr/bin/python3

import dask
import dask.dataframe as ddf
import pandas as pd
import numpy as np
import ast
import collections
import math
from glob import glob
import pickle
import re


df = pd.read_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/article-clustering/weight-for-each-term-each-page/')

df = df[df.index!='']

df = df[df.index!=' ']

df = df.reset_index().rename(columns={'index':'link'})

def cosine_similarity(counter1,counter2):
    c1squared= sum([a*a for a in counter1.values()])
    c2squared= sum([a*a for a in counter2.values()])
    c1c2=sum([counter1[k]*counter2[k]  for k in counter1.keys() if k in counter2])
    return c1c2/(math.sqrt(c1squared*c2squared))


def get_links(x):
    return x.set_index('link')[['wf-idf']].to_dict()['wf-idf']

page_links = df.groupby('page.title')



page_wlinkcounts = {}

for page in df['page.title'].unique():
    page_wlinkcounts[page] = get_links(page_links.get_group(page))
with open('result/page-wikicount-wf-idf.pkl', 'wb') as f:
    pickle.dump(page_wlinkcounts, f)
        
page_wlink_similarities={
    key1:{
        k:v for k,v in {
            key2: cosine_similarity(page_wlinkcounts[key1],page_wlinkcounts[key2]) 
            for key2 in page_wlinkcounts.keys() 
            if not key1.startswith('Talk:') and not key2.startswith('Talk:') 
                 and page_wlinkcounts[key1] and page_wlinkcounts[key2]
            }.items() if 1>v>0.001 
        } for key1 in page_wlinkcounts.keys()}

with open('result/article-similarity-wf-idf.pkl', 'wb') as f:
    pickle.dump(page_wlink_similarities, f)
    
df = pd.DataFrame.from_dict(page_wlink_similarities,orient='index')
df = df.fillna(0)
df = df[df.index.to_list()]
df.to_csv("result/article-similarity-matrix-wf-idf.csv", sep="\t",encoding='utf-16')
df.to_parquet('result/article-similarity-matrix-wf-idf')
