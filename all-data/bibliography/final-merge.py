#!/usr/bin/python3

import dask.dataframe as ddf
import collections
import pandas as pd
import json
import os
import numpy as np
import re
from Levenshtein import distance
import mwparserfromhell
from difflib import SequenceMatcher
from glob import glob
import time
import itertools
import argparse


path = 'ref_info_new/'
# all_folders = glob(path + '*')
# dfs = [ddf.read_parquet(f) for f in all_folders]
# df = ddf.concat(dfs, axis=0)
df = pd.read_parquet(path)
df = df[['url','unparsed','title','ref_id','publisher','author','journal','work','pmid','isbn','doi']]
# df = df.compute()


df = df.fillna('')
df = df[~df.index.duplicated(keep='first')]

# find refs with same url
def remove_url_prefix(url):
    if url == '':
        return ''
    t = re.compile(r"(https?://)?(www.)?")
    return t.sub('', url).strip().strip('/').lower()

df['url'] = df['url'].apply(remove_url_prefix)

def get_unique_index(index):
    return list(set(index))
df['ref_id'] = df.index
url = df.groupby('url').agg({'ref_id':get_unique_index})
# drop the column with no url
url = url[url.index != '']

# all pairs with same url
same_urls_pairs = url['ref_id'].to_list()

def sim_pairs(pairs):
    sim_pairs = {}
    for item in pairs:
        if len(item) == 1:
            continue
        else:
            val_index = 0
            for i in range(0,len(item)):
                if item[i].startswith('pmid'):
                    val_index = i
                    break
                if item[i].startswith('doi'):
                    val_index = i
                    break
                if item[i].startswith('isbn'):
                    val_index = i
                    break
                # tag name that does not contain .id.
                if re.search('.id.', item[i]) == None:
                    val_index = i
                    break
            for i in range(0,len(item)):
                if i == val_index:
                    continue
                else:
                    sim_pairs[item[i]] = item[val_index]
                
    return sim_pairs
url_dict = sim_pairs(same_urls_pairs)
matched_ids = {}
# add url_dict to matched_ids
matched_ids.update(url_dict)

# 2. For formatted refs, compare title, author, publisher, etc. 

formatted = df[df['unparsed'] == '']
candidate_list = formatted['title'].to_list()
formatted['df_index'] = range(0, len(formatted))

def similar(c1,c2):
    n = max(len(c1),len(c2))
    if n == 0:
        return 1
    return distance(c1.lower(),c2.lower())/n

vfnx = np.vectorize(similar)
matched_indices = []
def append_to_list(l):
    global matched_indices
    matched_indices += l
def find_possible_pairs_by_title(d):
    global matched_indices
    # if find a match in previous runs, continue
    # due to the parallel programming, some matched index will run before it is assigned as matched
    # remove duplicate later
    if d['df_index'] in matched_indices:
        return ''
    title = d['title']
    ratio = vfnx(candidate_list,title)
    sim_idx = [i for i, e in enumerate(ratio) if e < 0.3]
#     matched_indices += sim_idx
    append_to_list(sim_idx)
    return [formatted.index[i] for i in sim_idx]

from multiprocessing import Pool
from functools import partial

def parallelize(df,func,n_cores=16):
    df_split = np.array_split(df,n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func,df_split))
    pool.close()
    pool.join()
    return df
def run_on_subset(func, data_subset):
    return data_subset.apply(func, axis=1)
def parallelize_on_rows(data, func, num_of_processes=16):
    return parallelize(data, partial(run_on_subset, func), num_of_processes)

formatted['possible_pairs'] = parallelize_on_rows(formatted, find_possible_pairs_by_title)
possible_pairs = formatted['possible_pairs'].to_list()
possible_pairs = [i for i in possible_pairs if i if len(i) > 1]

def remove_duplicate_for_possible_pairs(possible_pairs):
    sort_inner_list = [i.sort() for i in possible_pairs]
    possible_pairs.sort()
    return list(possible_pairs for possible_pairs,_ in itertools.groupby(possible_pairs))
possible_pairs = remove_duplicate_for_possible_pairs(possible_pairs)

from itertools import combinations

def compare_cols(df,possible_pairs,metric,tol):
    sim_pairs = []
    
    for p in possible_pairs:
        tem_pairs = []
        x = list(combinations(p,2))

        for item in x:
            d1 = df.loc[item[0]][metric]
#             d1 = re.sub('[^A-Za-z0-9]+', '', d1)
            

            d2 = df.loc[item[1]][metric]
#             d2 = re.sub('[^A-Za-z0-9]+', '', d2)
            
            change_rate = similar(d1,d2)

            if change_rate <= tol:
                tem_pairs.append(item[0])
                tem_pairs.append(item[1])
            
        pairs = list(set(tem_pairs))
        sim_pairs.append(pairs)
            
    return [x for x in sim_pairs if x]

# compare some import parts
filter_publisher, filter_author, filter_journal, filter_work, filter_url = ([] for i in range(5))
if 'publisher' in formatted:
    filter_publisher = compare_cols(formatted,possible_pairs,'publisher',0.3) 
if 'author' in formatted:
    filter_author = compare_cols(formatted,possible_pairs,'author',0.3)
if 'journal' in formatted:    
    filter_journal = compare_cols(formatted,possible_pairs,'journal',0.3)
if 'work' in formatted:
    filter_work = compare_cols(formatted,possible_pairs,'work',0.3)
if 'url' in formatted:
    filter_url= compare_cols(formatted,possible_pairs,'url',0.3)
possible_pairs = list(set().union((tuple(row) for row in filter_publisher),(tuple(row) for row in filter_author),(tuple(row) for row in filter_journal),(tuple(row) for row in filter_work),(tuple(row) for row in filter_url)))
possible_pairs = [list(t) for t in possible_pairs]

replace_dict = sim_pairs(possible_pairs)      
replace_dict = {k:v for k,v in replace_dict.items() if v not in matched_ids}
matched_ids.update(replace_dict)


# # 3. compare content for all unformatted try to find similar one. 
# to speed up the program, do not consider unformatted one, not much compare to formatted refs
# try to find match with formatted refs in later step
unformatted = df[df['unparsed'] != '']
# matched_indices1 = []
# candidate_list_unparsed = unformatted['unparsed'].to_list()
# def append_to_list1(l):
#     global matched_indices1
#     matched_indices1 += l
# def find_possible_pairs_by_unparsed_content(d):
#     global matched_indices
#     # if find a match in previous runs, continue
#     # due to the parallel programming, some matched index will run before it is assigned as matched
#     # remove duplicate later
#     if d['df_index'] in matched_indices1:
#         return ''
#     title = d['unparsed']
#     ratio = vfnx(candidate_list_unparsed,title)
#     sim_idx = [i for i, e in enumerate(ratio) if e < 0.3]
# #     matched_indices += sim_idx
#     append_to_list1(sim_idx)
#     return [unformatted.index[i] for i in sim_idx]

# unformatted['df_index'] = range(0, len(unformatted))
# unformatted['possible_pairs'] = parallelize_on_rows(unformatted, find_possible_pairs_by_unparsed_content)
# possible_pairs = unformatted['possible_pairs'].to_list()
# possible_pairs = [i for i in possible_pairs if i if len(i) > 1]
# possible_pairs = remove_duplicate_for_possible_pairs(possible_pairs)

# replace_dict = sim_pairs(possible_pairs)
# replace_dict = {k:v for k,v in replace_dict.items() if v not in matched_ids}
# matched_ids.update(replace_dict)


# 4. For unformatted refs, find if there is a match with formatted refs (search in unforamtted refs with existing 
#    ids/authors in formatted)
def find_match_with_format(content,exist_field):
    for i in exist_field.index:
        x = exist_field[i]
        if x.lower() in content.lower():
            return i
    return ''
def form_sim_pairs(id1,id2,l):
    l.append([id1,id2])

# matched pmid
if 'pmid' in formatted:
    exist_pmid = formatted[formatted['pmid']!='']['pmid']
    unformatted['matched_pmid'] = unformatted.apply(lambda d: find_match_with_format(d['unparsed'],exist_pmid),axis=1)
    matched_pmid_df = unformatted[unformatted['matched_pmid']!='']
    matched_pmid = []
    c = matched_pmid_df.apply(lambda d: form_sim_pairs(d['ref_id'],d['matched_pmid'],matched_pmid),axis=1)
    replace_dict = sim_pairs(matched_pmid)
    replace_dict = {k:v for k,v in replace_dict.items() if v not in matched_ids}
    matched_ids.update(replace_dict)
    
# isbn
if 'isbn' in formatted:
    exist_isbn = formatted[formatted['isbn']!='']['isbn']
    unformatted['matched_isbn'] = unformatted.apply(lambda d: find_match_with_format(d['unparsed'],exist_isbn),axis=1)
    matched_isbn_df = unformatted[unformatted['matched_isbn']!='']
    matched_isbn= []
    e = matched_isbn_df.apply(lambda d: form_sim_pairs(d['ref_id'],d['matched_isbn'],matched_isbn),axis=1)
    replace_dict = sim_pairs(matched_isbn)
    replace_dict = {k:v for k,v in replace_dict.items() if v not in matched_ids}
    matched_ids.update(replace_dict)
    
# doi
if 'doi' in formatted:
    exist_doi = formatted[formatted['doi']!='']['doi']
    unformatted['matched_doi'] = unformatted.apply(lambda d: find_match_with_format(d['unparsed'],exist_doi),axis=1)

    matched_doi_df = unformatted[unformatted['matched_doi']!='']
    matched_doi= []
    f = matched_doi_df.apply(lambda d: form_sim_pairs(d['ref_id'],d['matched_doi'],matched_doi),axis=1)


    replace_dict = sim_pairs(matched_doi)
    replace_dict = {k:v for k,v in replace_dict.items() if v not in matched_ids}
    matched_ids.update(replace_dict)
    # unformatted = unformatted[~unformatted['ref_id'].isin(drop_index)]
    
new_ref_list = df[~df['ref_id'].isin(matched_ids.keys())]

p1 = 'result/'
json.dump(matched_ids, open(p1 + 'matched_ids', 'w'))
# x_df = ddf.from_pandas(new_ref_list,chunksize=10000)
# x_df.to_parquet(p1 + 'final_ref_list')
