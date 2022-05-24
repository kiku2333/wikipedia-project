#!/usr/bin/python3
import dask
import dask.dataframe as ddf
import os
from glob import glob
import pandas as pd
import numpy as np
import re

def main_program(original_folder):
    folder = original_folder + '/'
    # text, wikilinks, url a/m/r for each revision
    amr_info =  ddf.read_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/editor-profile/added-modified-removed-count/' + folder).compute()
    path = '/home/ubuntu/scratch/xinrui/mount-enwiki-columns/' + folder 
    
    all_folders = glob(path + '*')
        

    page_title_files = [s for s in all_folders if 'page.title' in s]
    titles = [ddf.read_parquet(file) for file in page_title_files]
    page_title = ddf.concat(titles).compute()
    
    contributor_files = [s for s in all_folders if 'contributor.username' in s]
    contributors = [ddf.read_parquet(file) for file in contributor_files]
    contributor = ddf.concat(contributors).compute()
    
    
    res = amr_info.join(page_title).join(contributor)
    fin = res.groupby(['contributor.username','page.title']).sum().reset_index()
    fin_df = ddf.from_pandas(fin,chunksize=10000)
    fin_df.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/editor-profile/amr-info-per-editor-per-page/' + folder)



def main():
    import argparse
    argparser= argparse.ArgumentParser('get number of added, modified, and removed info per editor per page')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)
    



if __name__ == '__main__':
    main()