#!/usr/bin/python3

import dask
import dask.dataframe as dd
import pandas as pd
import numpy as np
import time
import pickle
from glob import glob
import hashlib
import re


def compute_md5(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()



def reverts(text):
    for word in text.split():
        word = re.sub(r'[^a-zA-Z0-9 ]',r'',word).lower()
        if word == 'reverted' or word == 'undid' or word == 'undo' or word == 'rv':
            return True
    return False

class RevertGraph(object):
    def __init__(self,original_folder):
        self.folder = original_folder + '/'
        self.path  = '/home/ubuntu/scratch/xinrui/mount-enwiki-columns/' + self.folder
        self.prev_hash = {}
        self.matrix = pd.DataFrame()
        
        
    def get_kept_article_users(self):
        with open('../../all-data-article-clustering/articles-100-threshold.pkl' ,'rb') as f:
            selected_articles = pickle.load(f) 
        self.selected_articles_list = list(selected_articles)
        selected_users = pd.read_parquet('/home/ubuntu/scratch/xinrui/mount-files/selected-editors-based-on-cc')
        self.selected_users_list = list(selected_users.index)
        
        self.revt_graph = np.zeros((len(self.selected_users_list),len(self.selected_users_list)))
        
        
    def get_df(self):
        all_folders = glob(self.path + "*")
        
        # read files
        page_title_files = [s for s in all_folders if 'page.title' in s]
        titles = [dd.read_parquet(file) for file in page_title_files]
        page_title = dd.concat(titles)
        pt = page_title.compute()    
        kept_titles = pt[pt['page.title'].isin(self.selected_articles_list)]
        
        contributor_username = [s for s in all_folders if 'contributor.username' in s]
        usernames = [dd.read_parquet(file) for file in contributor_username]
        username = dd.concat(usernames)
        user = username.compute()
        
        article_user_info = kept_titles.join(user)
        article_user_info = article_user_info[article_user_info['contributor.username'].notnull()]
        article_user_info = article_user_info[article_user_info['contributor.username'].isin(self.selected_users_list)]
        
        revision_text = [s for s in all_folders if 'revision.text' in s]
        revision_texts = [dd.read_parquet(file) for file in revision_text]
        revision_text_pd = dd.concat(revision_texts).compute()
        
        
        revision_parent = [s for s in all_folders if 'revision.parentid' in s]
        revision_parents = [dd.read_parquet(file) for file in revision_parent]
        revision_parent_df = dd.concat(revision_parents).compute()
        
        time_stamp = [s for s in all_folders if 'revision.timestamp' in s]
        rv_times = [dd.read_parquet(file) for file in time_stamp]
        rv_time = dd.concat(rv_times).compute()
        
        revision_comments = [s for s in all_folders if 'revision.comment' in s]
        rv_comments = [dd.read_parquet(file) for file in revision_comments]
        rv_comment = dd.concat(rv_comments).compute()
        
        self.df = article_user_info.join(revision_text_pd)
        self.df = self.df.join(revision_parent_df)
        self.df = self.df.join(rv_time)
        self.df = self.df.join(rv_comment)
        self.df = self.df[~self.df.index.duplicated(keep='first')]

    def get_reverted_user(self,d):
        md5 = d['MD5']
        if md5 in self.prev_hash:
            parent_index = d['revision.parentid']
            if (parent_index in self.df.index):
                reverted_user = self.df.loc[parent_index]['contributor.username']
                if d['contributor.username'] != reverted_user:
                    return reverted_user 
        else:
            self.prev_hash[md5] = d.name
            if d['is_revert']:
                parent_index = d['revision.parentid']
                if (parent_index in self.df.index) and (md5 != self.df.loc[parent_index]['MD5']):
                    reverted_user = self.df.loc[parent_index]['contributor.username']
                    if d['contributor.username'] != reverted_user:
                        return reverted_user 
        return ''


    def create_revert_matrix(self,contributor,revt_user):
        i = self.selected_users_list.index(contributor)
        j = self.selected_users_list.index(revt_user)
        self.revt_graph[i][j] += 1

    def run(self):
        self.get_kept_article_users()
        self.get_df()
        self.df['MD5'] = self.df['revision.text'].apply(compute_md5)
        self.df['is_revert'] = self.df['revision.comment'].apply(reverts)
        self.df['reverted_user'] = self.df.apply(self.get_reverted_user,axis=1)
        self.df = self.df[self.df['reverted_user']!='']
        if not self.df.empty:
            self.df.apply(lambda d: self.create_revert_matrix(d['contributor.username'],d['reverted_user']),axis=1)
            self.matrix = pd.DataFrame(self.revt_graph,index=self.selected_users_list,columns=self.selected_users_list)
            self.matrix = self.matrix.loc[~(self.matrix==0).all(axis=1)]
            self.matrix = self.matrix.loc[:, ~(self.matrix == 0).all(axis=0)]



def main():
    import argparse
    argparser= argparse.ArgumentParser('create revert graph for selected editor and articles')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    x = RevertGraph(fn)
    x.run()
    x.matrix.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/warring-camps-threshold-100/revert-graph/' + x.folder[:-1])
    # rev1 revert rev2
    df = x.df[['revision.parentid','page.title']].reset_index().rename(columns={'index':'rev1','revision.parentid':'rev2'})
    df = x.df[['revision.parentid','page.title','contributor.username','reverted_user']].reset_index().rename(columns={'index':'rev1','revision.parentid':'rev2','contributor.username':'user1','reverted_user':'user2'})
    df.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/warring-camps-threshold-100/revert-index-info/' + x.folder[:-1])
    

if __name__ == '__main__':
    main()