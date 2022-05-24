#!/usr/bin/python3

import dask.dataframe as ddf
import collections
import pandas as pd
import numpy as np
import re
import mwparserfromhell
import multiprocessing
from glob import glob
import string
import argparse
import json
import pickle


def process_revision_text(text):
    parsed_wikicode = mwparserfromhell.parse(text)
    wikilinks = parsed_wikicode.filter_wikilinks()
    extracted_wikilinks = [i.title.lower() for i in wikilinks]
    return extracted_wikilinks


def main_program(original_folder):
    selected_editors = pd.read_parquet('/home/ubuntu/scratch/xinrui/mount-files/selected-editors-based-on-cc').index
    editors = list(selected_editors)

    folder = original_folder + '/'
    
    path = '/home/ubuntu/scratch/xinrui/mount-enwiki-columns/' + folder 
    all_folders = glob(path + "*")
    
    # read required info
    
    
    revision_text = [s for s in all_folders if 'revision.text' in s]
    revision_texts = [ddf.read_parquet(file) for file in revision_text]
    revision_text_pd = ddf.concat(revision_texts).compute()
    
    user_files = [s for s in all_folders if 'contributor.username' in s]
    users = [ddf.read_parquet(file) for file in user_files]
    user = ddf.concat(users)
    user = user.compute()
    
    
    page_title_files = [s for s in all_folders if 'page.title' in s]
    titles = [ddf.read_parquet(file) for file in page_title_files]
    page_title = ddf.concat(titles)
    page_title = page_title.compute()
    
    page_info = revision_text_pd.join(user).join(page_title)
    page_info = page_info[page_info['contributor.username'].isin(editors)]
    with open('articles-100-threshold.pkl' ,'rb') as f:
        selected_articles = pickle.load(f)
        
    page_info = page_info[page_info['page.title'].isin(selected_articles)]
    page_info['wikilinks'] = ddf.from_pandas(page_info,npartitions=4*multiprocessing.cpu_count()).map_partitions(lambda d: d['revision.text'].apply(process_revision_text)).compute(scheduler='processes')
    fin = page_info[['wikilinks']]
    final_df = ddf.from_pandas(fin,npartitions=4*multiprocessing.cpu_count())    
    final_df.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/wikilinks-new-selected-articles/' + folder, object_encoding='json')
    
    
    

def main():   
    argparser= argparse.ArgumentParser('get wikilinks for selected editors and articles')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)
    



if __name__ == '__main__':
    main()