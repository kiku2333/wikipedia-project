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
import argparse





def main_program(original_folder):
    folder = original_folder + '/'
#     wiki_df = pd.read_parquet('result/wikilink-count')
    links = ddf.read_parquet('../../../intermediate-result-new-select-editors/wikilinks-new-selected-articles/'+ folder) .compute()
    path = '/home/ubuntu/scratch/xinrui/mount-enwiki-columns/' + folder 
    all_folders = glob(path + "*")

    page_title_files = [s for s in all_folders if 'page.title' in s]
    titles = [ddf.read_parquet(file) for file in page_title_files]
    page_title = ddf.concat(titles)
    page_title = page_title.compute()
    
    links = links.join(page_title)

    x = links.groupby('page.title').agg({'wikilinks':'sum'})
    
    x['wikilinks'] = x['wikilinks'].map(lambda x : collections.Counter(x))

#     kept_links = wiki_df.index

    res = {}

    def count_links(df):
        links = df['wikilinks']
        title = df.name
        for k,v in links.items():
            if k not in res:
                res[k] = {}
            res[k][title] = v
            
    x.apply(count_links,axis=1)
    with open('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/article-clustering/links-count/' + original_folder + '.pkl', 'wb') as f:
        pickle.dump(res, f)
    

def main():   
    argparser= argparse.ArgumentParser('get wikilinks count')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)
    



if __name__ == '__main__':
    main()