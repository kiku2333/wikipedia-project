#!/usr/bin/python3

import pandas as pd
import numpy as np
import dask
import dask.dataframe as ddf
import argparse




def main_program(original_folder):
    selected_editors = pd.read_parquet('/home/ubuntu/scratch/xinrui/mount-files/selected-editors-based-on-cc').index
    path = '/home/ubuntu/scratch/xinrui/intermediate-result/user-page-rev/'
    x = pd.read_parquet(path + original_folder)
    x = x[x.index.get_level_values(0).isin(selected_editors)]
    x = x[x.index.get_level_values(1).map(lambda a: not a.startswith('Talk:'))]
    x['num.contributions'] = x['index'].map(len)
    x = x[['num.contributions']]
    x.to_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/select-articles/contribution-info/' + original_folder)

    
        
    

def main():   
    argparser= argparse.ArgumentParser('get contribution for select editors for all articles')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)
    



if __name__ == '__main__':
    main()