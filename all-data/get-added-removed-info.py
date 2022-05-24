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


def process_revision_text(text):
    parsed_wikicode = mwparserfromhell.parse(text)
    # wikilinks
    wikilinks = parsed_wikicode.filter_wikilinks()
    # url
    urls = parsed_wikicode.filter_external_links()
    # revision text
    content = parsed_wikicode.strip_code()
    content = content.replace('\n','.')
    # remove the space before dot
    content = re.sub(r'\s+([.](?:\s|$))', r'\1',content)
    # content = re.split(r'(?<=[^A-Z].[.?])+[ ]*(?=[A-Z=<])',content)
    content = re.split(r'(?<=.[.?])+[ ]*(?=[A-Za-z=<])',content)
    content = [i.translate(str.maketrans('', '', string.punctuation)) for i in content]
    extracted_wikilinks = [str(i) for i in wikilinks]
    extracted_urls = [str(i) for i in urls]
    extracted_text = [" ".join(i.split()) for i in content if i]
    return pd.Series([extracted_wikilinks,extracted_urls,extracted_text],index=['wikilinks','url','text'])


def main_program(original_folder):
    selected_articles = pd.read_parquet('/home/ubuntu/xinrui/process-all-wiki/c-score/result/selected_articles_based_on_c_score.parquet').index
    selected_talks = list(map(lambda x: "Talk:" + str(x), selected_articles))
    selected_pages = list(selected_articles) + selected_talks
    selected_editors = pd.read_parquet('/home/ubuntu/xinrui/selected-editors-based-on-cc').index
    editors = list(selected_editors)

    folder = original_folder + '/'
    
    path = '/home/ubuntu/enwiki-columns/' + folder 
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
    
    
    page_info = page_title.join(user)
    parents_files = [s for s in all_folders if 'revision.parentid' in s]
    parents = [ddf.read_parquet(file) for file in parents_files]
    parent_ids = ddf.concat(parents)
    parent_ids = parent_ids.compute()

    page_info = page_info[page_info['page.title'].isin(selected_pages)]
    wanted_revisions = page_info[page_info['contributor.username'].isin(editors)].index
    kept_wanted_revisions = page_info[page_info.index.isin(wanted_revisions)]
    kept_wanted_revisions = kept_wanted_revisions.join(parent_ids)
    parent_ids_list = list(kept_wanted_revisions['revision.parentid'])
    wanted_revisions = list(wanted_revisions)
    # keep the selected editors and corresponding parent revisions for comparsion
    kept_indexes = list(set(wanted_revisions + parent_ids_list))
    df = page_info[page_info.index.isin(kept_indexes)]
    df = df.join(revision_text_pd)
    df = df.join(parent_ids)
    
    # if dataframe is empty (no selected editors and articles)
    if df.empty:
        return
    
    df[['wikilinks','url','text']] = ddf.from_pandas(df,npartitions=4*multiprocessing.cpu_count()).\
map_partitions(lambda d: d['revision.text'].apply(process_revision_text)).compute(scheduler='processes')
    
    
    
    def find_added_removed(d,col):
        curr_id = d.name
        if d['contributor.username'] not in editors:
            return pd.Series([None,None],index=[col+'.added',col+'.removed'])
        prev_id = d['revision.parentid']
        curr_content = collections.Counter(df.loc[curr_id][col])
        prev_content = collections.Counter()

        if prev_id in df.index:
            prev_content = collections.Counter(df.loc[prev_id][col])

        added = curr_content - prev_content
        removed = prev_content - curr_content

        return pd.Series([added,removed],index=[col+'.added',col+'.removed'])
    
    df[['text.added','text.removed']] = df.apply(find_added_removed,args=('text',),axis=1)
    df[['wikilinks.added','wikilinks.removed']] = df.apply(find_added_removed,args=('wikilinks',),axis=1)
    df[['url.added','url.removed']] = df.apply(find_added_removed,args=('url',),axis=1)
    
    res = df[df['contributor.username'].isin(editors)]
    final = res[['text.added','text.removed','wikilinks.added','wikilinks.removed','url.added','url.removed']]
    final_df = ddf.from_pandas(final,npartitions=4*multiprocessing.cpu_count())    
    final_df.to_parquet('/home/ubuntu/intermediate-result-new-select-editors/added-removed-info/' + folder, object_encoding='json')
        
    

def main():   
    argparser= argparse.ArgumentParser('get added removed info for selected editors and articles')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)
    



if __name__ == '__main__':
    main()