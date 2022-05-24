#!/usr/bin/python3
import dask
import dask.dataframe as ddf
import os
from glob import glob
import re
import pandas as pd
import numpy as np
import collections
import json
import mwparserfromhell
from multiprocessing import  Pool
import argparse

argparser= argparse.ArgumentParser('processed the revision text')
argparser.add_argument('infile')
args=argparser.parse_args()

folder = args.infile
path = '/home/ubuntu/enwiki-columns/' + folder + '/'

directory = '/home/ubuntu/intermediate-result/all-data/added-removed-without-filters/' + folder
if not os.path.exists(directory):
    os.makedirs(directory)

def find_bznumber(text):
    match = re.search(r'(\bbz(\d+--\d+)\b)',text).group(0)
    return match
x = glob(path + '*title*')
all_bz_number = [find_bznumber(i) for i in x]


prefixes = ['Talk:','User:','User talk:','Wikipedia','Wikipedia talk:','File:','File talk:','MediaWiki','MediaWiki talk:',\
           'Template:','Template talk:','Help:','Help talk:','Category:','Category talk:']
def is_kept(title):
    return not title.startswith(tuple(prefixes))

def process_text(text):
    text = text.split("== References ==")[0]
    text = text.split("==External links==")[0]
    text = text.replace('\n',' ')
    # remove wikilinks "[[xxx]]"
    text = re.sub("\[\[.*?\]\]", '',text)
    # remove "{{xxx}}"
    text = re.sub("\{\{.*?\}\}", '',text)
    text = re.sub('<ref.*?</ref>', '', text)
    text = re.sub(r'[^\w\s]',' ',text)
    text = text.split(' ')
    text = [i for i in text if i]
    text = collections.Counter(text)
    text = dict(text)
    return str(text)

def process_func(bz):
    titles = glob(path + '*'+bz+'*page.title*')
    df = ddf.read_parquet(titles).compute()
    df['is_kept'] = df['page.title'].apply(is_kept)
    df = df[df['is_kept']].drop(columns={'is_kept'})
    parents_files = glob(path + '*'+bz+'*revision.parentid*')
    df = df.join(ddf.read_parquet(parents_files).compute())
    text = glob(path + '*'+bz+'*revision.text*')
    df = df.join(ddf.read_parquet(text).compute())
    editor = glob(path + '*'+bz+'*contributor.username*')
    df = df.join(ddf.read_parquet(editor).compute())
    df['processed_text'] = df['revision.text'].apply(process_text)
    df = df.drop(columns={'revision.text'})
    df.to_parquet(directory + '/' + bz[3:])
    
p = Pool(processes=16)
x = p.map(process_func, all_bz_number)
p.close()
p.join()
