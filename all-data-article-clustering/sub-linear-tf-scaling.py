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
import argparse
import json




def main_program(original_file):
    path = '/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/article-clustering/links-count/'
    with open(path + original_file, 'rb') as f:
        loaded_dict = pickle.load(f) 
    df = pd.DataFrame.from_dict(loaded_dict, orient="index").stack().to_frame()
    df = df.rename(columns={0:'tf'}) 
    df = df.reset_index()
    df = df.rename(columns={'level_0':'wikilink','level_1':'page.title'})
    df_idf = pd.read_parquet('result/df-idf-for-each-term')
    
    df = df.set_index('wikilink').join(df_idf)
    df['wf'] = df['tf'].map(lambda x: 1 + math.log(x + 1) if x > 0 else 0)
    df['wf-idf'] = df['wf'] * df['idf']
    res = df[['page.title','wf-idf']]
    res.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/article-clustering/weight-for-each-term-each-page/' + original_file[:-4])

    

def main():   
    argparser= argparse.ArgumentParser('get weight for each term each page using sub linear tf scaling')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)
    



if __name__ == '__main__':
    main()