#!/usr/bin/python3
import dask
import dask.dataframe as ddf
import os
from glob import glob
import re
import pandas as pd
import numpy as np
import time
import pickle
import hashlib
import mwparserfromhell
import _pickle as cPickle


def estimate_crc(input_data,model):
    

    predict = model.predict(input_data)
    predict = round(predict[0],2)
    return 0 if predict < 0 else predict

# filter out bots
def is_bot(text):
    text = text.split(' ')
    for item in text:
        if item.lower().endswith('bot'):
            return True
    return False
        



def user_crc(folder):
    parquetdir = '/home/ubuntu/intermediate-result/'
    title_dir = title_dir = '/home/ubuntu/splitted_titles_for_each_folder/'
    with open(os.path.join(title_dir,folder + '_article.txt'), 'rb') as f:
        titles = pickle.load(f)
        
    talks = list(map(lambda x: "Talk:" + str(x), titles))

    all_titles = titles + talks

    all_talk_files = glob(title_dir + '*talk*')
    
    talk_folders = []
    for i in all_talk_files:
        append = False
        with open(i, 'rb') as f:
            talk_titles = pickle.load(f)
            for title in talk_titles:
                if title in talks:
                    append = True
                    break
            if append:
                talk_folders.append(re.search('enwiki(.*)',i)[0][:-9])
                
    all_files = [parquetdir +  s for s in talk_folders]
    if parquetdir + folder not in all_files:
        all_files.append(parquetdir + folder)
    res = pd.DataFrame()
    for file in all_files:
        test = ddf.read_parquet(os.path.join(parquetdir,file,'revision.count')).compute()
        test = test.loc[test.index.isin(all_titles)]
        test = test.join(ddf.read_parquet(os.path.join(parquetdir,file,'links.count')).compute())
        test = test.join(ddf.read_parquet(os.path.join(parquetdir,file,'minor')).compute())
        test = test.join(ddf.read_parquet(os.path.join(parquetdir,file,'page.length')).compute())
        test = test.join(ddf.read_parquet(os.path.join(parquetdir,file,'reverts_new')).compute())
        test = test.join(ddf.read_parquet(os.path.join(parquetdir,file,'anon.edits')).compute())
        test = test.join(ddf.read_parquet(os.path.join(parquetdir,file,'unique.editors')).compute())
        if 'anon_edits' in test.columns:
            test = test.rename(columns={'anon_edits':'anon.edits'})
        if 'unique_editors' in test.columns:
            test = test.rename(columns={'unique_editors':'unique.editors'})
        res = pd.concat([res,test])

    res = res[~res.index.duplicated(keep='first')]
    
    
    article_df = res.loc[~res.index.str.startswith('Talk:')]
    talk_df = res.loc[res.index.str.startswith('Talk:')]
    talk_df.index = talk_df.index.str.replace('Talk:','')
    talk_df.columns = ['talk.' + str(col) for col in talk_df.columns]
    final = article_df.join(talk_df)
    final = final.fillna(0).astype('int')
    
    with open('regression-model-all-data.pkl', 'rb') as fid:
        model = cPickle.load(fid)
    final['estimated_crc'] = final.apply(lambda df: estimate_crc([[df['revision.count'],df['page.length'],df['unique.editors'],
        df['links.count'],df['anon.edits'],df['minor'],df['reverts'],df['talk.revision.count'],df['talk.page.length'],
        df['talk.unique.editors'],df['talk.links.count'],df['talk.anon.edits'],df['talk.minor']]],model),axis=1)
    
    
    article_c = final[['estimated_crc']]
    article_c = ddf.from_pandas(article_c,chunksize=10000)
    article_c.to_parquet(os.path.join('/home/ubuntu/intermediate-result/article_c_score',folder))
    
    crc_dict = final['estimated_crc'].to_dict()
    
    
    
    # user c score
    
    user_contribution_info = pd.read_parquet(os.path.join('/home/ubuntu/intermediate-result/user-page-rev',folder))
    all_users = user_contribution_info.index.get_level_values(0).unique()
    
    def t1(df):
        title = df.name
        if title.startswith('Talk:'):
            return 0
        c_article = crc_dict[title] 
        if c_article == 0:
            return 0
        return c_article * len(df['index'])/final.loc[title]['revision.count']
    
    user_crc = {}
    def get_user_crc(user):
        curr_df = user_contribution_info.loc[user]
        x = curr_df.apply(t1,axis=1)
        user_crc[user] = sum(x)
        
        
    x = all_users.map(get_user_crc)
    
    user_df = pd.DataFrame.from_dict(user_crc, orient='index', columns=['c_score'])
    user_df = ddf.from_pandas(user_df,chunksize=10000)
    user_df.to_parquet(os.path.join('/home/ubuntu/intermedaite-result/user_c_score',folder))
    
    
    

    



def main():
    import argparse
    argparser= argparse.ArgumentParser('compute article and editor c score')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    x = user_crc(fn)
    



if __name__ == '__main__':
    main()