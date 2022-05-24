#!/usr/bin/python3

import dask
import dask.dataframe as ddf
import pandas as pd
import numpy as np
import ast
import collections
import math
from glob import glob
import argparse
import pickle




def main_program(original_folder):
    folder = original_folder + '/'
    parquetdir = '../../../intermediate-result-new-select-editors/wikilinks-new-selected-articles/'
    links = pd.read_parquet(parquetdir + folder,engine='fastparquet')
    all_folders = glob('/home/ubuntu/scratch/xinrui/mount-enwiki-columns/' + folder + '*page.title*')
    titles = [ddf.read_parquet(file) for file in all_folders]
    page_title = ddf.concat(titles).compute()
    page_texts = links.join(page_title)
    page_texts = page_texts.groupby('page.title')
    page_wlinkcounts={page: page_texts.get_group(page)['wikilinks'].apply(
        lambda x: collections.Counter(x)
        ).sum() 
    for page in  page_texts.groups.keys() }
    
    
    with open('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/article-clustering/article-links/' + original_folder + '.pkl', 'wb') as f:
        pickle.dump(page_wlinkcounts, f)
    
        
    

def main():   
    argparser= argparse.ArgumentParser('get wikilinks for selected editors and articles')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)
    



if __name__ == '__main__':
    main()