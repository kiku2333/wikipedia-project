#!/usr/bin/python3
# need to change the path

import dask
import dask.dataframe as ddf
import os
import re
import pandas as pd
import numpy as np
import pickle
from sklearn.metrics.pairwise import cosine_similarity
import lda
from sklearn.feature_extraction.text import CountVectorizer
import sys


def is_article(text):
    return re.search('Talk:', text) == None

def my_tokenizer(text):
    text = re.split(r'([a-zA-Z1-9]+)', text)
    # remove words whose length is less than 3
    text = [t.strip() for t in text if len(t.strip()) > 2]
    return text

class myThread (object):
    def __init__(self, user):
        self.user = user
        self.contributions = pd.read_parquet('editor_sig_contrib_info').loc[user]
        self.contributions = self.contributions[self.contributions['contributions']>1]
        self.parquetdir = '/home/ubuntu/intermediate-result/all-data-extracted-page-info/'
        self.clust_n = {}
        self.clust_d = {}
    
    def remove_talk_pages(self):
        self.contributions = self.contributions[self.contributions.index.map(is_article)]
    
    def get_all_folders(self):
        with open('/home/ubuntu/xinrui/intermediate-result/title-look-up.pickle', 'rb') as fp:
            title_lookup = pickle.load(fp)
        self.all_folders = []
        for title in self.contributions.index:
            if title not in title_lookup:
                continue
            folder = title_lookup[title]
            if folder in self.all_folders:
                continue
            self.all_folders.append(folder)
    
    def get_article_info(self):
        self.res = pd.DataFrame()
        for file in self.all_folders:
            test = ddf.read_parquet(os.path.join(self.parquetdir,file,'revision.count')).compute()
            test = test.loc[test.index.isin(self.contributions.index)]
            test = test.join(ddf.read_parquet(os.path.join(self.parquetdir,file,'processed-page-content')).compute())
            x = ddf.read_parquet(os.path.join('/home/ubuntu/intermediate-result/article_c_score',file)).compute()
            test = test.join(x)
            self.res = pd.concat([self.res,test])
        self.res = self.res[~self.res.index.duplicated(keep='first')]
        self.res = self.res[self.res['estimated_crc']!=0]


    def create_lda_model(self):
        n_topics = 100
        model = lda.LDA(n_topics=n_topics, n_iter=100,alpha=50/n_topics,eta=0.1)
        vectorizer = CountVectorizer(min_df=0.01,stop_words='english',tokenizer=lambda text: my_tokenizer(text))
        content = self.res['processed_text'].to_list()
        X = vectorizer.fit_transform(content)
        vocab = vectorizer.get_feature_names()
        model.fit(X)
        self.doc_topic = model.doc_topic_
        
    def get_page_similarity(self):
        self.titles = self.res.index
        n = len(self.titles)

        page_similarity = np.zeros([n,n])
        for i in range(len(self.doc_topic)):
            topic = self.doc_topic[i]
            page_similarity[i,:] = cosine_similarity([topic],self.doc_topic)[0]
        self.page_similarity_df = pd.DataFrame(page_similarity,index=self.titles,columns=self.titles)
        
    def get_l(self):
        self.res = self.res.join(self.contributions)
        self.res['l'] = self.res['contributions']/self.res['revision.count'] * self.res['estimated_crc']
        self.l = self.res[['l']]['l'].values
        n = len(self.titles)
        li = self.l.reshape(n,1)
        lj = self.l.reshape(1,n)
        self.lij = li.dot(lj)
        
    def get_cc(self):
        self.w = self.page_similarity_df.values
        n = len(self.titles)
        self.result = [self.compute_product(i) for i in range(n)]
        return sum(self.result)
    
    def compute_product(self,k):
        n = len(self.titles)
        wki = self.w[k,:].reshape(n,1)
        wkj = self.w[k,:].reshape(1,n)
        wki_wkj = wki.dot(wkj)

        common = self.lij * wki_wkj
        num = common * self.w
        d = common
        num_sum = num.sum()#.compute()
        d_sum = d.sum()#.compute()

        clust = num_sum/d_sum
        return self.l[k] * clust    
   
    def run(self):
        self.remove_talk_pages()
        self.get_all_folders()
        self.get_article_info()
        self.create_lda_model()
        self.get_page_similarity()
        self.get_l()
        cc = self.get_cc()

        with open('cc-score-new-1.csv','a') as f:
            f.write("{},{}".format(self.user,cc))
            f.write("\n")


def main():
    import argparse
    argparser= argparse.ArgumentParser('compute cc score')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    editor = args.infile
    x = myThread(editor)
    x.run()


if __name__ == '__main__':
    main()