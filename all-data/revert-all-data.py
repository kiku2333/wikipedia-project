#!/usr/bin/python3

import dask
import dask.dataframe as ddf
import os
from glob import glob
import re
import pandas as pd
import numpy as np
import hashlib

def is_reverts(text):
    for word in text.split():
        word = re.sub(r'[^a-zA-Z0-9 ]',r'',word).lower()
        if word == 'reverted' or word == 'undid' or word == 'undo' or word == 'rv':
            return True
    return False

def compute_md5(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def main_program(original_folder):
    folder = original_folder + '/'

    
    path = '/home/ubuntu/scratch/xinrui/mount-enwiki-columns/' + folder 

    all_folders = glob(path + '*')
        
    page_title_files = [s for s in all_folders if 'page.title' in s]
    titles = [ddf.read_parquet(file) for file in page_title_files]
    page_title = ddf.concat(titles).compute()

    contributor_files = [s for s in all_folders if 'contributor.username' in s]
    contributors = [ddf.read_parquet(file) for file in contributor_files]
    contributor = ddf.concat(contributors).compute()
    
    selected_editors = pd.read_parquet('/home/ubuntu/scratch/xinrui/mount-files/selected-editors-based-on-cc').index
    editors = list(selected_editors)
    df = page_title.join(contributor)
    df = df[df['contributor.username'].isin(editors)]
    
    articles = pd.read_parquet('/home/ubuntu/scratch/xinrui/mount-files/process-all-wiki/c-score/result/selected_articles_based_on_c_score.parquet').index
    talks = 'Talk:' + articles
    articles = articles.append(talks)
    articles = list(articles)
    df = df[df['page.title'].isin(articles)]
    
    
    comment_files = [s for s in all_folders if 'revision.comment' in s]
    comments = [ddf.read_parquet(file) for file in comment_files]
    comment = ddf.concat(comments).compute()

    df = df.join(comment)
    
    revision_files = [s for s in all_folders if 'revision.text' in s]
    revisions = [ddf.read_parquet(file) for file in revision_files]
    revision = ddf.concat(revisions).compute()
    
    df = df.join(revision)
    
    
    df['MD5'] = df['revision.text'].apply(compute_md5)
    
    # for each md5, drop the first index, so if only one revision, then this will not be count
    # drop_list stores the first index of each md5
    # in future step, when working with comment, only look at index in drop_list
    # this is because all others are counted in md5 part
    
    drop_list = []
    def get_drop_index(df):
        drop_list.append(df.index.values[0])
    
    
    x = df.groupby('MD5').apply(get_drop_index)
    md5_info = df[~df.index.isin(drop_list)]
    md5_info = md5_info.groupby(['contributor.username','page.title','MD5']).agg({'revision.text':'count'}).rename(columns={'revision.text':'reverts'})
    md5_info = md5_info.groupby(['contributor.username','page.title']).agg({'reverts':'sum'})
    
    # comment
    comment_info = df[df.index.isin(drop_list)]
    comment_info['is_revert'] = comment_info['revision.comment'].apply(is_reverts)
    comment_info = comment_info.groupby(['contributor.username','page.title']).agg({'is_revert':'sum'}).rename(columns={'is_revert':'reverts'})
    
    reverts = pd.concat([md5_info,comment_info])
    reverts = reverts.reset_index().groupby(['contributor.username','page.title']).agg({'reverts':'sum'}).reset_index()
    reverts.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/editor-profile/revert-info/' + original_folder)






def main():
    import argparse
    argparser= argparse.ArgumentParser('get added removed info for selected editors and articles')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)
    



if __name__ == '__main__':
    main()
