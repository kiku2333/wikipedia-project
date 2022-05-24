#!/usr/bin/python3
import dask.dataframe as ddf
import dask.distributed
import collections
import pandas as pd
import json
import os
import numpy as np
import re
from glob import glob


def rules(text):
    if text:
        return re.search('wp:',text.lower())
    return None

def arbcom(text):
    if text:
        return re.search('wp:(arbcom|ac|arb)',text.lower())
    return None

def ban(text):
    if text:
        return re.search('wp:(ban|banpol)',text.lower())
    return None

def is_talk(title):
    return title.startswith('Talk:')

def main_program(original_folder):
    folder = original_folder + '/'
    amr = ddf.read_parquet('../../../intermediate-result-new-select-editors/editor-profile/added-modified-removed-text-wikilinks-url/'+ folder).compute()
    wiki_added = amr[['new.wikilinks.added']]
    
    path = '/home/ubuntu/scratch/xinrui/mount-enwiki-columns/' + folder 
    all_folders += glob(path + '*')

    page_title_files = [s for s in all_folders if 'page.title' in s]
    titles = [ddf.read_parquet(file) for file in page_title_files]
    page_title = ddf.concat(titles)
    page_title = page_title.compute()

    contributor_files = [s for s in all_folders if 'contributor.username' in s]
    contributors = [ddf.read_parquet(file) for file in contributor_files]
    contributor = ddf.concat(contributors).compute()
    
    df = wiki_added.join(page_title).join(contributor)
    df['is_talk'] = df['page.title'].apply(is_talk)
    talk = df[df['is_talk']]
    talk['new.wikilinks.added'] = talk['new.wikilinks.added'].map(' \t '.join)
    talk['rules'] = talk['new.wikilinks.added'].apply(rules).notnull()
    talk['arbcom'] = talk['new.wikilinks.added'].apply(arbcom).notnull()
    talk['ban'] = talk['new.wikilinks.added'].apply(ban).notnull()
    
    selected_editors = pd.read_parquet('/home/ubuntu/scratch/xinrui/mount-files/selected-editors-based-on-cc').index
    editors = list(selected_editors)
    
    talk = talk[talk['contributor.username'].isin(editors)]
    res = talk.groupby(['contributor.username','page.title']).sum()
    res = res.drop(columns={'is_talk'})
    res = res.reset_index()
    
    res.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/editor-profile/later-info/' + original_folder)
    
    
    

def main():
    import argparse
    argparser= argparse.ArgumentParser('get number of rules, arbcom, and bans for talk pages per editor per page')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)
    



if __name__ == '__main__':
    main()