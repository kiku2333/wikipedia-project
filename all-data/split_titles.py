#!/usr/bin/python3

import os
import pickle


def split(folder):
    title_dir = '/home/ubuntu/scratch/xinrui/process-all-wiki/title_list/'
    with open(os.path.join(title_dir,folder + '.txt'), 'rb') as f:
        titles = pickle.load(f)
        talk = []
        article = []
        def is_talk(s):
            if s.startswith('Talk:'):
                talk.append(s)
            else:
                article.append(s)
                
        x = [is_talk(x) for x in titles]
        
        with open('/home/ubuntu/intermediate-result/splitted_titles_for_each_folder/' + folder +'_article.txt', "wb") as fp:   #Pickling
            pickle.dump(article, fp)
        with open('/home/ubuntu/intermediate-result/splitted_titles_for_each_folder/' + folder +'_talk.txt', "wb") as fp:   #Pickling
            pickle.dump(talk, fp)
    



def main():
    import argparse
    argparser= argparse.ArgumentParser('extract and store Wikipedia revision wikilinks')
    argparser.add_argument('infile')
    args=argparser.parse_args()
    fn= args.infile
    
    x = split(fn)
    



if __name__ == '__main__':
    main()
        