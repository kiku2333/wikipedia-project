#!/usr/bin/python3

import dask
import dask.dataframe as ddf
import os
from glob import glob
import re
import pandas as pd
import numpy as np
import math
import collections
import json
import argparse
import mwparserfromhell

argparser= argparse.ArgumentParser('find added, modified and removed refs for selected editors and articles')
argparser.add_argument('infile')
args=argparser.parse_args()

original_folder = args.infile

folder = original_folder + '/'
x = ddf.read_parquet('/home/ubuntu/intermediate-result-new-select-editors/bibliography/refs-added-removed-info/' + folder).compute()

from difflib import SequenceMatcher

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def convert_dict_to_list(d):
    refs = []
    for k,v in d.items():
        tags = re.findall('<ref[^>]*>[^<]*', k)
        tag_names = [''.join(re.search('<ref[^>]*>',i)[0].split('/')) + '</ref>' for i in tags if ('/>' in i)]
        tag_refs = [re.findall('<ref[^>]*>[^<]*', i)[0] + '</ref>' for i in tags if not ('/>' in i)]
        tag_refs += tag_names
        refs += tag_refs
        
    return refs

def find_modification(df,col):
    added = df[col + '.added']
    removed = df[col + '.removed']
    
    if (len(added) == 0) & (len(removed) == 0):
        return [[],[],[]]
    elif len(added) == 0:
        return [[],[],convert_dict_to_list(df[col + '.removed'])]
    elif len(removed) == 0:
        return [convert_dict_to_list(df[col + '.added']),[],[]]
    else:
        # use beautifulSoup to re-extract the refs
        # a: added list
        # r: removed list
        a = convert_dict_to_list(added)
        r = convert_dict_to_list(removed)
        
        # some refs may be same, so remove them
        added_list = [elem for elem in a if elem not in r]
        removed_list = [elem for elem in r if elem not in a]
        
        matched_indexes = []
        similar_pairs = []
        for i in range(len(added_list)):
            val = added_list[i]
            sim = list(map(lambda x: similar(x,val),removed_list))
            if len(sim) > 0:
                max_sim = max(sim)
                max_index = sim.index(max(sim))
                if (max_sim > 0.8) and (max_index not in matched_indexes):
                    similar_pairs.append([i,max_index])
                    matched_indexes.append(max_index)
        
        add_index = [item[0] for item in similar_pairs]
        removed_index = [item[1] for item in similar_pairs]
        add_matched = [added_list[i] for i in add_index]
        removed_matched = [removed_list[i] for i in removed_index]
        
        modified_res = [{removed_list[item[1]]:added_list[item[0]]} for item in similar_pairs]
        added_res = list(set(added_list)^set(add_matched))
        removed_res = list(set(removed_list)^set(removed_matched))
        return [added_res,modified_res,removed_res]
        

x[['refs.added','refs.modified','refs.removed']] = x.apply(lambda d: find_modification(d,'refs'),axis=1, result_type="expand")

res_df = ddf.from_pandas(x,chunksize=10000)
res_df.to_parquet('/home/ubuntu/intermediate-result-new-select-editors/bibliography/refs-added-modified-removed/' + folder, object_encoding='json')