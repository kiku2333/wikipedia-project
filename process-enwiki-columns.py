#!/usr/bin/python3

import dask
import dask.dataframe as dd
import os
from glob import glob
import re
import pandas as pd
import numpy as np
import time
import pickle
import hashlib
import mwparserfromhell
import json


def filter_title(title):
    list_str = ['^Media:','^Special:','^User:','^User talk:','^Wikipedia:','^Wikipedia talk:','^File:'\
              ,'^File talk:','^MediaWiki:','^MediaWiki talk:','^Template:','^Template talk:','^Help:',\
              '^Help talk:','^Category:','^Category talk:','^Portal:','^Portal talk:','^Book:','^Book talk:',\
              '^Draft:','^Draft talk:','^Education Program:','^Education Program talk:','^TimedText:','^TimedText talk:',\
              '^Module:','^Module talk:','^Gadget:','^Gadget talk:','^Gadget definition:','^Gadget definition talk:']
    match = re.findall(r"(?=("+'|'.join(list_str)+r"))",str(title))
    return len(match) == 0
def filter_revert(text):
    if text:
        return re.search('revert',text.lower())
    return None
def filter_crc(text):
    if text:
        return re.search(r'({{controversial}})',text.lower())
    return None

def find_unique_editors(c):
    return len(list(set(c['contributor'])))

def article_length(text):
    return len(text)

def compute_md5(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def find_links(text):
    return '\t'.join(str(x) for x in mwparserfromhell.parse(text).filter_wikilinks())

def get_num_links(text):
    return len(text.split('\t'))




class ProcessParquet(object):
    def __init__(self, folder):
        self.original_folder = folder
        self.folder = folder + '/'
        self.all_folders = []
        self.final_path = '/home/ubuntu/scratch/xinrui/process-all-wiki/intermediate-result/' + self.folder
        self.path  = '/home/ubuntu/scratch/enwiki-columns/' + self.folder
        self.kept_titles = pd.DataFrame()
        self.kept_index = []
        self.revision_text_pd = pd.DataFrame()
        self.controversial_list = []
        self.title_index_info = {}
        
    def get_all_parquet_folders(self):
        self.all_folders = glob(self.path + "*")
        
    def filter_page(self):
        page_title_files = [s for s in self.all_folders if 'page.title' in s]
        titles = [dd.read_parquet(file) for file in page_title_files]
        page_title = dd.concat(titles)
        pt = page_title.compute()
        
        pt['keep'] = pt['page.title'].apply(filter_title)
        self.kept_titles = pt[pt['keep']]
        self.kept_titles = self.kept_titles.drop(columns={'keep'})
        self.kept_index = self.kept_titles.index.tolist()
        
        # store titles for reference?
        titles = self.kept_titles['page.title'].unique().tolist()
        with open('/home/ubuntu/scratch/xinrui/process-all-wiki/title_list/' + self.original_folder +'.txt', "wb") as fp:   #Pickling
            pickle.dump(titles, fp)


    def revisions(self):
        revisions = self.kept_titles.groupby('page.title').agg({'page.title':['count']})
        revisions.columns = revisions.columns.droplevel(0)
        revisions = revisions.rename(columns={'count':'revision.count'})
        revisions_df = dd.from_pandas(revisions,chunksize=1000)
        revisions_df.to_parquet(os.path.join(self.final_path,'revision.count/'))
#         return revisions
        
    def anon_edits(self):
        ip_files = [s for s in self.all_folders if 'contributor.ip' in s]
        ips = [dd.read_parquet(file) for file in ip_files]
        anon_editor = dd.concat(ips)
        anon = anon_editor.compute()
        kept_anon = anon[anon.index.isin(self.kept_index)]
        page_ip_info = kept_anon.join(self.kept_titles)
        anons = page_ip_info.groupby('page.title').agg({'contributor.ip':['count']})
        anons.columns = anons.columns.droplevel(0)
        anons = anons.rename(columns={'count':'anon.edits'})
        anons_df = dd.from_pandas(anons,chunksize=1000)
        anons_df.to_parquet(os.path.join(self.final_path,'anon.edits/'))
#         return anons
    
    # page length and number of links
    def most_current_revision(self):
        time_files = [s for s in self.all_folders if 'revision.timestamp' in s]
        timestamp = [dd.read_parquet(file) for file in time_files]
        time_df = dd.concat(timestamp)
        time_pd = time_df.compute()
        
        revision_text = [s for s in self.all_folders if 'revision.text' in s]
        revision_texts = [dd.read_parquet(file) for file in revision_text]
        self.revision_text_pd = dd.concat(revision_texts).compute()
        
        revision_time_info = (self.kept_titles.join(time_pd)).join(self.revision_text_pd)
        max_time = revision_time_info.groupby('page.title').agg({'revision.timestamp':['max']})
        max_time.columns = max_time.columns.droplevel(0)
        max_time = max_time.reset_index()
        revision_time_info = revision_time_info.reset_index()
        revision_time_info = revision_time_info.drop(columns={'index'})
        
        revision_time_info = max_time.merge(revision_time_info,right_on=['page.title','revision.timestamp'],
                                            left_on=['page.title','max'], how='left').set_index('page.title')
        revision_time_info = revision_time_info.drop(columns={'max'})
        
        revision_time_info['page.length'] = revision_time_info['revision.text'].apply(article_length)
        revision_time_info['controversial'] = revision_time_info['revision.text'].apply(filter_crc).notnull()
        
        self.controversial_list = revision_time_info[revision_time_info['controversial']].index.to_list()
#         #save controversial list        
        with open('/home/ubuntu/scratch/xinrui/process-all-wiki/controversial_titles/' + self.original_folder +'.txt', "wb") as fp:   #Pickling
            pickle.dump(self.controversial_list, fp)
        
        
        page_length = revision_time_info.drop(columns={'revision.timestamp','revision.text','controversial'})
        page_length_df = dd.from_pandas(page_length,chunksize=1000)
        page_length_df.to_parquet(os.path.join(self.final_path,'page.length/'))
        
        #get the content for the most recent version of the page
        page_content =  revision_time_info.drop(columns={'revision.timestamp','page.length','controversial'})
        page_content_df = dd.from_pandas(page_content,chunksize=1000)
        page_content_df.to_parquet(os.path.join(self.final_path,'page.content/'))
        
        #links
        revision_time_info['links'] = revision_time_info['revision.text'].apply(find_links)
        revision_time_info['links.count'] = revision_time_info['links'].apply(get_num_links)
        links_count = revision_time_info.drop(columns={'revision.timestamp','revision.text','links','controversial','page.length'})
        links_count_df = dd.from_pandas(links_count,chunksize=1000)
        links_count_df.to_parquet(os.path.join(self.final_path,'links.count/'))
        link_info = revision_time_info.drop(columns={'revision.timestamp','revision.text','links.count','controversial','page.length'})
        link_info_df = dd.from_pandas(link_info,chunksize=1000)
        link_info_df.to_parquet(os.path.join(self.final_path,'links.info/'))
        
    def get_reverts(self):
        revert_files = [s for s in self.all_folders if 'revision.comment' in s]
        reverts = [dd.read_parquet(file) for file in revert_files]
        revert = dd.concat(reverts)
        rv = revert.compute()
        kept_rv = rv[rv.index.isin(self.kept_index)]
        reverts_info = kept_rv.join(self.kept_titles)
        reverts_info = reverts_info.join(self.revision_text_pd)
        
        reverts_info['md5'] = reverts_info['revision.text'].apply(compute_md5)
        reverts_md5 = reverts_info.groupby(['page.title','md5']).agg({'revision.text':'count'}).rename(columns={'revision.text':'reverts'})
        reverts_md5['reverts'] -= 1
        reverts_md5 = reverts_md5.reset_index().groupby('page.title').agg({'reverts':'sum'})
        reverts_info['revert'] = reverts_info['revision.comment'].apply(filter_revert).notnull()
        revert_rvs = reverts_info.groupby('page.title').agg({'revert':['sum']}).astype('int')
        revert_rvs.columns = revert_rvs.columns.droplevel(0)
        revert_rvs = revert_rvs.rename(columns={'sum':'reverts'})
        
        merged = pd.concat([reverts_md5,revert_rvs])

        final = merged.groupby('page.title').agg({'reverts':'sum'}).astype('int')
        final_df  = dd.from_pandas(final,chunksize=1000)
        final_df.to_parquet(os.path.join(final_path,'reverts_new/'))

        
    def get_crc(self):
        revision_text_filtered = self.kept_titles.join(self.revision_text_pd)
        controversial_df = revision_text_filtered[revision_text_filtered['page.title'].isin(self.controversial_list)]
#         controversial_df['crc'] = controversial_df['revision.text'].apply(filter_crc).notnull()
        y = controversial_df['revision.text'].apply(filter_crc).notnull()
        y = pd.DataFrame(y).rename(columns={'revision.text':'crc'})
        controversial_df = controversial_df.join(y)
        
        crc = controversial_df.groupby('page.title').agg({'crc':['sum']}).astype('int')
        crc.columns = crc.columns.droplevel(0)
        crc = crc.rename(columns={'sum':'crc'})
        crc_df = dd.from_pandas(crc,chunksize=1000)
        crc_df.to_parquet(os.path.join(self.final_path,'crc/'))
    
    def get_unique_editors(self):
        contributor_username = [s for s in self.all_folders if 'contributor.username' in s]
        usernames = [dd.read_parquet(file) for file in contributor_username]
        username = dd.concat(usernames)
        
        user = username.compute()
        
        contributors = user.rename(columns={'contributor.username':'contributor'})
        contributors = self.kept_titles.join(contributors)
        contributors = contributors[contributors['contributor'].notnull()]
        ue = contributors.groupby('page.title').apply(find_unique_editors)
        ue_df = ue.to_frame(name='unique.editors')
        ue_dask = dd.from_pandas(ue_df,chunksize=1000)
        ue_dask.to_parquet(os.path.join(self.final_path,'unique.editors/'))
        
    def get_minor(self):
        minor_path = '../enwiki-minor/' + self.folder
        all_minor_files = glob(minor_path + "*")
        minors = [dd.read_parquet(file) for file in all_minor_files]
        minor = dd.concat(minors).compute()
        
        minor_info = self.kept_titles.join(minor)
        minor_info = minor_info.groupby('page.title').agg({'minor':['sum']}).astype('int')
        minor_info.columns = minor_info.columns.droplevel(0)
        minor_info = minor_info.rename(columns={'sum':'minor'})
        minor_df = dd.from_pandas(minor_info,chunksize=1000)
        minor_df.to_parquet(os.path.join(self.final_path,'minor/'))


                

        
    def run(self):
        self.get_all_parquet_folders()
        self.filter_page()
        self.revisions()
        self.anon_edits()
        self.get_minor()
        self.most_current_revision()
        self.get_reverts()
        self.get_crc()
        self.get_unique_editors()

def main():
    import argparse
    argparser= argparse.ArgumentParser('extract and store Wikipedia revision wikilinks')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    x = ProcessParquet(fn)
    x.run()
    



if __name__ == '__main__':
    main()
        
    
    