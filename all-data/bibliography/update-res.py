#!/usr/bin/python3

import dask.dataframe as ddf
import collections
import pandas as pd
import json
import argparse

argparser= argparse.ArgumentParser('update res list')
argparser.add_argument('infile')
args=argparser.parse_args()

folder = args.infile
path = '/home/ubuntu/intermediate-result-new-select-editors/bibliography/ref-page-user-info/'

df = pd.read_parquet(path + folder)

with open('result/matched_ids') as data_file:    
    matched_ids = json.load(data_file)
    
def update_res(rid):
    if rid in matched_ids.keys():
        return matched_ids[rid]
    return rid

df['new_id'] = df['ref_id'].apply(update_res)
df = df.drop(columns={'ref_id'}).rename(columns={'new_id':'ref_id'})
df.to_parquet('/home/ubuntu/intermediate-result-new-select-editors/bibliography/bibliography-final-res/' + folder)
