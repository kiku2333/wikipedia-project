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
from difflib import SequenceMatcher

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


def convert_dict_to_list(d):
    res = []
    for item in d:
        res.append(item)
    return res

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
        added_list = convert_dict_to_list(added)
        removed_list = convert_dict_to_list(removed)
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

def get_len(df):
    df['text.added'] = len(df['new.text.added'])
    df['text.modified'] = len(df['new.text.modified'])
    df['text.removed'] = len(df['new.text.removed'])
    df['wikilinks.added'] = len(df['new.wikilinks.added'])
    df['wikilinks.modified'] = len(df['new.wikilinks.modified'])
    df['wikilinks.removed'] = len(df['new.wikilinks.removed'])
    df['url.added'] = len(df['new.url.added'])
    df['url.modified'] = len(df['new.url.modified'])
    df['url.removed'] = len(df['new.url.removed'])
    return df

def main_program(original_folder):
    folder = original_folder + '/'
    x = ddf.read_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/added-removed-info/' + folder).compute()
    x[['new.wikilinks.added','new.wikilinks.modified','new.wikilinks.removed']] = x.apply(lambda d: find_modification(d,'wikilinks'),axis=1, result_type="expand")
    x[['new.url.added','new.url.modified','new.url.removed']] = x.apply(lambda d: find_modification(d,'url'),axis=1, result_type="expand")
    x[['new.text.added','new.text.modified','new.text.removed']] = x.apply(lambda d: find_modification(d,'text'),axis=1, result_type="expand")
    
    res = x[['new.text.added','new.text.modified','new.text.removed','new.wikilinks.added','new.wikilinks.modified','new.wikilinks.removed','new.url.added','new.url.modified','new.url.removed']]
    
    res_df = ddf.from_pandas(res,chunksize=10000)
    res_df.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/editor-profile/added-modified-removed-text-wikilinks-url/' + folder, object_encoding='json')
    
    fin = res.apply(get_len,axis=1)
    fin = fin[['text.added','text.modified','text.removed','wikilinks.added','wikilinks.modified','wikilinks.removed','url.added','url.modified','url.removed']]
    fin_df = ddf.from_pandas(fin,chunksize=10000)
    
    fin_df.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/editor-profile/added-modified-removed-count/' + folder)
    






def main():
    import argparse
    argparser= argparse.ArgumentParser('get number of added, modified, andremoved info for selected editors and articles')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)
    



if __name__ == '__main__':
    main()