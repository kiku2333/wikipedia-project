#!/usr/bin/python3

import dask
import dask.dataframe as dd
import os
from glob import glob
import re
import pandas as pd
import numpy as np



def filter_title(title):
    list_str = ['^Media:','^Special:','^User:','^User talk:','^Wikipedia:','^Wikipedia talk:','^File:'\
              ,'^File talk:','^MediaWiki:','^MediaWiki talk:','^Template:','^Template talk:','^Help:',\
              '^Help talk:','^Category:','^Category talk:','^Portal:','^Portal talk:','^Book:','^Book talk:',\
              '^Draft:','^Draft talk:','^Education Program:','^Education Program talk:','^TimedText:','^TimedText talk:',\
              '^Module:','^Module talk:','^Gadget:','^Gadget talk:','^Gadget definition:','^Gadget definition talk:']
    match = re.findall(r"(?=("+'|'.join(list_str)+r"))",str(title))
    return len(match) == 0


def get_all_revisions(rev):
    return list(rev)

def get_user_page_rev(original_folder):
    folder = original_folder + '/'
    path  = '/home/ubuntu/scratch/enwiki-columns/' + folder
#     minor_path  = '/home/ubuntu/scratch/xinrui/enwiki-minor/' + folder
    
    all_folders = glob(path  + '*')
    all_minor_folders = glob(minor_path + '*')
    
    page_title_files = [s for s in all_folders if 'page.title' in s]
    titles = [dd.read_parquet(file) for file in page_title_files]
    page_title = dd.concat(titles)
    page_title = page_title.compute()

    page_title['keep'] = page_title['page.title'].apply(filter_title)
    kept_titles = page_title[page_title['keep']]
    kept_titles = kept_titles.drop(columns={'keep'})

    user_files = [s for s in all_folders if 'contributor.username' in s]
    users = [dd.read_parquet(file) for file in user_files]
    user = dd.concat(users)
    user = user.compute()
    
    
#     # this part gets revs that are not minor
#     minor_files  = [s for s in all_minor_folders if 'minor' in s]
#     minors = [dd.read_parquet(file) for file in minor_files]
#     minor = dd.concat(minors)
#     minor = minor.compute()
    
#     kept_titles = kept_titles.join(minor)
#     kept_titles = kept_titles[kept_titles['minor'] == False]
    

#     page_user = kept_titles.merge(user,left_index=True,right_index=True)
    
    x = user.reset_index().groupby(['contributor.username','page.title'])['index'].apply(get_all_revisions)
    
    x = pd.DataFrame(x)
    
    x.to_parquet('/home/ubuntu/scratch/xinrui/process-all-wiki/get-info-for-entire-wiki-pages/user-page-rev/' + original_folder)
#     x.to_parquet('/home/ubuntu/scratch/xinrui/process-all-wiki/get-info-for-entire-wiki-pages/user-page-rev-not-minor/' + original_folder)
    

def main():
    import argparse

    argparser= argparse.ArgumentParser('get user contribution info')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    folder= args.infile
    x = get_user_page_rev(folder)

if __name__ == '__main__':
    main()

