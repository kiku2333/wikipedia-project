#!/usr/bin/python3
import dask
import dask.dataframe as ddf
import os
from glob import glob
import re
import pandas as pd
import numpy as np
import argparse

argparser= argparse.ArgumentParser('processed the revision text')
argparser.add_argument('infile')
args=argparser.parse_args()
folder = args.infile


directory = '/home/ubuntu/intermediate-result/all-data/added_token_len/' + folder
df = pd.read_parquet(directory)
path = '/home/ubuntu/enwiki-columns/' + folder + '/'

titles = glob(path + '*title*')
titles = ddf.read_parquet(titles).compute()
df = df.join(titles).drop(columns={'dir0'})
editors = glob(path + '*contributor.username*')
editors = ddf.read_parquet(editors).compute()
df = df.join(editors).drop(columns={'dir0'})
x = df[df['added_len'] >= 20]

x.to_parquet('/home/ubuntu/intermediate-result/all-data/sig-contributions-all-data/' + folder)