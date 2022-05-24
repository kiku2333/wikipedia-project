#!/usr/bin/python3

import dask.dataframe as ddf
import pandas as pd
import numpy as np
import re
import os
import time
from glob import glob
from wikiwho_wrapper import WikiWho
import mwparserfromhell
import string
import requests
import math


def main_program(original_folder):
    folder = original_folder + '/'
    ww = WikiWho('wikiProject','Apple1234')
    kept_users = pd.read_parquet('/home/ubuntu/scratch/xinrui/mount-files/selected-editors-based-on-cc').index
    kept_users_list = list(kept_users)

    folder = original_folder + '/'
    path = '/home/ubuntu/scratch/xinrui/mount-enwiki-columns/' + folder 

    all_folders = glob(path + '*')
        
    selected_articles = pd.read_parquet('/home/ubuntu/scratch/xinrui/mount-files/process-all-wiki/c-score/result/selected_articles_based_on_c_score.parquet').index
    articles = list(selected_articles)
    page_title_files = [s for s in all_folders if 'page.title' in s]
    titles = [ddf.read_parquet(file) for file in page_title_files]
    page_title = ddf.concat(titles).compute()
    page_title = page_title[page_title['page.title'].isin(articles)]
    
    timestamp_files = [s for s in all_folders if 'revision.timestamp' in s]
    timestamp = [ddf.read_parquet(file) for file in timestamp_files]
    timestamp = ddf.concat(timestamp).compute()
    
    df = page_title.join(timestamp)
    df_max_time = df.groupby('page.title').agg({'revision.timestamp':'max'}).reset_index()
    x = df_max_time.merge(df.reset_index(),on=['page.title','revision.timestamp'])
    x = x.set_index('page.title')
    x = x[~x.index.duplicated(keep='first')]
    
    contributor_files = [s for s in all_folders if 'contributor.username' in s]
    contributors = [ddf.read_parquet(file) for file in contributor_files]
    contributor = ddf.concat(contributors).compute()
     
    contributor_id_files = [s for s in all_folders if 'contributor.id' in s]
    contributors_id = [ddf.read_parquet(file) for file in contributor_id_files]
    contributor_id = ddf.concat(contributors_id).compute()
    
    contributor = contributor.join(contributor_id)
    contributor_info = contributor[~contributor['contributor.username'].duplicated(keep='first')].set_index('contributor.id')
    contributor_info = contributor_info.to_dict()['contributor.username']
    fin_df = page_title.join(contributor)
    
    sr = {}
    hl = {}
    
    # x.index remove talks
    all_titles = list(x.index)
    all_articles = [i for i in all_titles if not i.startswith('Talk:')]
    

    for title in all_articles:
        # sr
        # get content for most current content
        max_rev = x.loc[title]['index']
        a = fin_df[fin_df['page.title'] == title]
        all_indices = list(a.index)
    #     response = ww.api.specific_rev_content_by_rev_id(max_rev,title)
#         try:
#             response = ww.api.specific_rev_content_by_rev_id(max_rev,title)
#         except requests.exceptions.RequestException as e:
#             print(e)
#             continue

        # a flag that indicate if we want to continue this loop or start next iteration with new title
        cont = True
        max_times = 0
        response = ''
        while True:
            # if we tried more than 10 times for a request, get the folder name and title name, and handle it later
            if max_times > 10:
                cont = False
                print('max time exceed ' + original_folder + ' ' + title)
                break
            try:
                response = ww.api.specific_rev_content_by_rev_id(max_rev,title)
            except requests.exceptions.RequestException as e:
                # 429: too many requests error, sending too many requests with in a second
                # wait for 10 seconds and try it again
                if e.response.status_code == 429:
                    time.sleep(10)
                    max_times += 1
                    continue
                # for any other type of error, not try again, and break the current while loop
                # set the cont flag to false, so we will continue on the next title
                else:
                    cont = False
                    break
            # if the response works ok, continue
            else:
                break
         
        if 'revisions' not in response:
            print('no revisions tag ' + original_folder + ' ' + title)
            cont = False
            
        if cont == False:    
            continue
        
        

        all_tokens = response['revisions'][0][max_rev]['tokens']
        kept_editor_count = {}
        for i,v in enumerate(d for d in all_tokens):
            if str(v['o_rev_id']) not in all_indices:
                continue
            editor = v['editor']
            if editor not in contributor_info:
                continue
            editor_name = contributor_info[editor]
            if editor_name not in kept_users_list:
                continue
            if editor_name in kept_editor_count:
                kept_editor_count[editor_name] += 1
            else:
                kept_editor_count[editor_name] = 1

        # get total number of token added for each editor
        

    #     response = ww.api.all_content(title)
#         try:
#             response = ww.api.all_content(title)
#         except requests.exceptions.RequestException as e:  # This is the correct syntax
#             print(e)
#             continue

        cont = True
        max_times = 0
        response = ''
        while True:
            if max_times > 10:
                cont = False
                print('max time exceed ' + original_folder + ' ' + title)
                break
            try:
                response = ww.api.all_content(title)
            except requests.exceptions.RequestException as e:
                if e.response.status_code == 429:
                    time.sleep(10)
                    max_times += 1
                    continue
                else:
                    cont = False
                    break
            else:
                break
                
            
        if cont == False:    
            continue


            
        all_tokens_added = {}
        all_tokens = response['all_tokens']
        for i in all_tokens: 
            # if the token was added after the most current content we have
            # then do not count it
            if str(i['o_rev_id']) not in all_indices:
                continue
            editor_id = i['editor']
            if editor_id not in contributor_info:
                continue
            editor_name = contributor_info[editor_id]
            if editor_name not in kept_users_list:
                continue
            if editor_name in all_tokens_added:
                all_tokens_added[editor_name] += 1
            else:
                all_tokens_added[editor_name] = 1

    #     res = {}
    #     for i,v in kept_editor_count.items():
    #         res[i] = v/all_tokens_added[i]
    #     sr[title] = res

        sr[title] = {'all_added':all_tokens_added,'survived':kept_editor_count}


        ########### end of sr ##################

        # hl
        a = a.join(timestamp)
        a = a[~a.index.duplicated(keep='first')]
        a = a.sort_values('revision.timestamp')
        a.insert(0, 'nth_revision', range(0, len(a)))


        # res stores hl for each rev (per token)
        res = {}

        def add_to_res(ind,val):
            if ind in res:
                res[ind].append(val)
            else:
                res[ind] = [val]

        max_n = a['nth_revision'].max()

        for v in all_tokens:
            if str(v['o_rev_id']) not in all_indices:
                continue
            editor_id = v['editor']
            if editor_id not in contributor_info:
                continue
            editor_name = contributor_info[editor_id]

            # check if editor is kept
            if editor_name not in kept_users_list:
                continue

            # in and out only look at the revisions we have
        #     ins = v['in']
        #     ins = [i for i in ins if str(i) in all_indices]
            outs = v['out']
            outs = [i for i in outs if str(i) in all_indices]

            o_rev = v['o_rev_id']

            if len(outs) == 0:
                age = max_n - a.loc[str(o_rev)]['nth_revision']
                add_to_res(o_rev,age)
                continue

            deleted_n = a.loc[str(outs[-1])]['nth_revision']
            curr_n = a.loc[str(o_rev)]['nth_revision']
            add_to_res(o_rev,deleted_n - curr_n)

        curr_hl = {}
        for i,v in res.items():
            v.sort()
            mid = math.ceil(len(v)/2) - 1
            curr_hl[str(i)] = v[mid]

        hl[title] = curr_hl
    
    all_added_df = pd.concat({
        k: pd.DataFrame.from_dict(v['all_added'], 'index') for k, v in sr.items()
    }, 
    axis=0).reset_index().rename(columns={'level_0':'page.title','level_1':'contributor.username',0:'all_added_tokens'})
    survive_df = pd.concat({
        k: pd.DataFrame.from_dict(v['survived'], 'index') for k, v in sr.items()
    }, 
    axis=0).reset_index().rename(columns={'level_0':'page.title','level_1':'contributor.username',0:'survived_tokens'})
    
    all_added_df = all_added_df.set_index(['contributor.username','page.title'])
    survive_df = survive_df.set_index(['contributor.username','page.title'])
    res = all_added_df.join(survive_df).fillna(0)
    res['all_added_tokens'] = res['all_added_tokens'].astype('int')
    res['survived_tokens'] = res['survived_tokens'].astype('int')
    res.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/warring-camps/sr/' + original_folder)
    
    fin_hl = pd.concat({
        k: pd.DataFrame.from_dict(v, 'index') for k, v in hl.items()
    }, 
    axis=0).reset_index().rename(columns={'level_0':'page.title','level_1':'index',0:'hl'}).set_index('index')
    
    df = fin_hl.drop(columns={'page.title'}).join(fin_df)
    df = df[~df.index.duplicated(keep='first')]
    df = df.drop(columns={'contributor.id'})
    df['hl'] = df['hl'].astype('int')
    df.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/warring-camps/hl/' + original_folder)
    

def main():
    import argparse
    argparser= argparse.ArgumentParser('get sr and hl using WikiWho')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)
    



if __name__ == '__main__':
    main()