#!/usr/bin/python3

import dask
import dask.dataframe as ddf
import pandas as pd
import numpy as np
from string import punctuation
import collections
import re
import mwparserfromhell
from nltk.corpus import stopwords
import codecs
import os
from glob import glob
import pickle



def main_program(original_folder):
    folder = original_folder + '/'
    add_removed = ddf.read_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/editor-profile/added-modified-removed-text-wikilinks-url/' + folder).compute()
    add_removed = add_removed[['new.text.added','new.text.modified','new.text.removed']]
    
    path = '/home/ubuntu/scratch/xinrui/mount-enwiki-columns/' + folder  + '/'
    all_folders = glob(path + '*')
        
    user_group = pd.read_csv('result/all-data-editor-cluster-result.csv', sep="\t",encoding='utf-16').set_index('contributor.username')
    page_title_files = [s for s in all_folders if 'page.title' in s]
    titles = [ddf.read_parquet(file) for file in page_title_files]
    page_title = ddf.concat(titles).compute()

    contributor_files = [s for s in all_folders if 'contributor.username' in s]
    contributors = [ddf.read_parquet(file) for file in contributor_files]
    contributor = ddf.concat(contributors).compute()
    
    df = page_title.join(contributor).join(add_removed)
    df = df[~df.index.duplicated(keep='first')]
    kept_editors = user_group.index.to_list()
    df = df[df['contributor.username'].isin(kept_editors)]
    with open('../../all-data-article-clustering/articles-100-threshold.pkl' ,'rb') as f:
        selected_articles = pickle.load(f) 
    df = df[df['page.title'].isin(selected_articles)]
    df = df.fillna('')
    
    def cleanString(strval):
        return "".join(" " if i in punctuation else i for i in strval.strip(punctuation))
    
    cachedStopWords = stopwords.words("english")
    # extend the stopwords
    cachedStopWords.extend(['also','ref','name'])
    
    def process_text(t):
#         t = codecs.decode(t, 'unicode_escape')
        t = cleanString(t).lower()
        t = re.sub(' +', ' ', t).split()
        t = [word for word in t if word not in cachedStopWords]
        t = [i for i in t if not i.isdigit()]
        return collections.Counter(t)

    def get_added_removed_words(x):
        added = x['new.text.added']
        modified = x['new.text.modified']
        removed = x['new.text.removed']
        add = collections.Counter()
        remove = collections.Counter()
        # modified, check words difference
        if len(modified)!= 0:
            for items in modified:
                # a: old, b: new
                a = list(items.keys())[0]
                b = list(items.values())[0]
                a = process_text(a)
                b = process_text(b)
                add += (b - a)
                remove += (a - b)
        if len(added) != 0:
            for sentence in added:
                parsed_wikicode = mwparserfromhell.parse(sentence)
                content = parsed_wikicode.strip_code()
                content = process_text(content)
                add += (content)
        if len(removed) != 0:
            for sentence in removed:
                parsed_wikicode = mwparserfromhell.parse(sentence)
                content = parsed_wikicode.strip_code()
                content = process_text(content)
                remove += (content)
        return add,remove
    
    
    df[['added_words','removed_words']] = df.apply(get_added_removed_words,axis=1,result_type="expand")
    df = df.merge(user_group,left_on='contributor.username',right_index=True)
    x = df.groupby(['group','page.title']).agg({'new.text.modified':'count','added_words':'sum','removed_words':'sum'})
    x = x[x['new.text.modified']>=100]
    x = x.drop(columns='new.text.modified')
    x = x.reset_index().drop(columns='page.title')
    
    x.to_csv('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/warring-camps-threshold-100/words-added-removed-all-revisions/' + original_folder + '.csv',sep = '\t', encoding = 'utf-16')
    
    

    
    

    


def main():
    import argparse
    argparser= argparse.ArgumentParser('words added removed per group per article')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)

if __name__ == '__main__':
    main()