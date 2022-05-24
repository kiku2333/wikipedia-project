#!/usr/bin/python3

import dask
import dask.dataframe as ddf
import os
from glob import glob
import re
import pandas as pd
import numpy as np


def main_program(original_folder):
    folder = original_folder + '/'
    path = '/home/ubuntu/scratch/xinrui/mount-enwiki-columns/' + folder 
    file_index = []
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
    
    df = df[df['page.title'].isin(articles)]
    
    res = df.groupby(['contributor.username','page.title']).agg({'page.title':'count'}).rename(columns={'page.title':'count'})
    res.reset_index().to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/editor-profile/contribution-info/' + original_folder)
    
    



def main():
    import argparse
    argparser= argparse.ArgumentParser('get number of contributions for selected editors and articles')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)
    



if __name__ == '__main__':
    main()