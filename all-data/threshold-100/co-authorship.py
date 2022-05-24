#!/usr/bin/python3

import dask.dataframe as ddf
import pandas as pd
import json
import os
import numpy as np
import pickle
import itertools




def main_program(original_folder):
    path = '/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/editor-profile/contribution-info-all-articles/'
    df = pd.read_parquet(path + original_folder)
    with open('../../all-data-article-clustering/articles-100-threshold.pkl' ,'rb') as f:
        selected_articles = pickle.load(f) 
    
    selected_talks = {'Talk:' + i for i in selected_articles}
    selected_pages = list(selected_articles) + list(selected_talks)
    df = df[df['page.title'].isin(selected_pages)]
    df = df.set_index(['contributor.username','page.title'])
    selected_editors = pd.read_parquet('/home/ubuntu/scratch/xinrui/mount-files/selected-editors-based-on-cc').index
    selected_editors = list(selected_editors)
    n = len(selected_editors)
    
    # number of shared articles for each editor pairs
    co_involvement = np.empty((n,n))
    # counting the number of contributions
    weighted_co_involvement = np.empty((n,n))

    np.fill_diagonal(co_involvement,np.nan)
    np.fill_diagonal(weighted_co_involvement,np.nan)

    x = df.groupby(['page.title','contributor.username']).sum()
    
    def get_coauthorship(article):
        all_editor_info = x.loc[article]
        editors = all_editor_info.index
        for i in itertools.combinations(editors,2):
            e1 = i[0]
            e2 = i[1]
            index1 = selected_editors.index(e1)
            index2 = selected_editors.index(e2)
            co_involvement[index1][index2] += 1 
            co_involvement[index2][index1] += 1 

            # weighted
            edits1 = all_editor_info.loc[e1]['count']
            edits2 = all_editor_info.loc[e2]['count']
            min_edits = min(edits1,edits2)
            weighted_co_involvement[index1][index2] += min_edits
            weighted_co_involvement[index2][index1] += min_edits
            
    a = x.index.get_level_values(0).unique().map(get_coauthorship)
    matrix = pd.DataFrame(co_involvement,index=selected_editors,columns=selected_editors)
    weighted_matrix = pd.DataFrame(weighted_co_involvement,index=selected_editors,columns=selected_editors)
    
    matrix.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/co-authorship/co-authorship/' + original_folder)
    weighted_matrix.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/co-authorship/weighted-co-authorship/' + original_folder)
    
    
def main():
    import argparse
    argparser= argparse.ArgumentParser('get coauthorship for selected editor and articles')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn = args.infile
    
    main_program(fn)

if __name__ == '__main__':
    main()