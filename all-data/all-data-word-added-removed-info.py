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




def main_program(original_folder):
    folder = original_folder + '/'
    revert_info = pd.read_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/warring-camps/revert-index-info/' + original_folder)
    add_removed = ddf.read_parquet('/home/ubuntu/scratch/xinrui/intermediate-result-new-select-editors/editor-profile/added-modified-removed-text-wikilinks-url/' + folder).compute()
    
    add_removed = add_removed[['new.text.added','new.text.modified','new.text.removed']]
    
    
    x = revert_info.merge(add_removed,left_on='rev1',right_index=True)
    
    def cleanString(strval):
        return "".join(" " if i in punctuation else i for i in strval.strip(punctuation))

    cachedStopWords = stopwords.words("english")

    # extend the stopwords
    cachedStopWords.extend(['also','ref','name'])
    
    def process_text(t):
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
    
    x[['added_words','removed_words']] = x.apply(get_added_removed_words,axis=1,result_type="expand")
    x = x.drop(columns={'new.text.added','new.text.modified','new.text.removed'})
    x.to_csv('../../../intermediate-result-new-select-editors/warring-camps/words-added-removed-info-based-on-revert-graph-csv/' + original_folder + '.csv',sep = '\t', encoding = 'utf-16')
    x.to_parquet('../../../intermediate-result-new-select-editors/warring-camps/words-added-removed-info-based-on-revert-graph/' + original_folder)
    
    
    

def main():
    import argparse
    argparser= argparse.ArgumentParser('words added removed per group per article')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    main_program(fn)

if __name__ == '__main__':
    main()