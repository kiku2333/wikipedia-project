#!/usr/bin/python3


import os
import bz2 as bzip
from bs4 import BeautifulSoup
from xml.dom import pulldom
import lxml
import pandas
import dask.dataframe as ddf



def getSoup(node,doc):
    doc.expandNode(node)
    return BeautifulSoup(node.toxml(),'xml')

def streamWikipediaRevisions(fn):
    with bzip.open(fn,'rt',encoding='utf-8') as chunk:
        doc= pulldom.parse(chunk)
        revcount=0
        currentpage={}
        for event, node in doc:
            if event == pulldom.START_ELEMENT:
                if node.tagName == 'page':
                    currentpage={}
#                 elif node.tagName == "title":
#                     currentpage['title'] = getSoup(node,doc).text
#                     #print(currentpage)
#                 elif node.tagName == 'id':
#                     currentpage['id']= getSoup(node,doc).text
                elif node.tagName == 'revision':
                    rev= getRevision(getSoup(node,doc))
                    for key,val in currentpage.items():
                        rev['page.'+key]=val
#                     rev['filename']= fn
                    rev['revision.fileindex']=revcount
                    revcount += 1
                    #print(rev.keys())
                    yield rev                
                
def getRevision(revisionXML):
    revisionTimestamp= revisionXML.timestamp.text
    revisionID= revisionXML.id.text
    revisionParentID= ''
    if revisionXML.parentid:
        revisionParentID= revisionXML.parentid.text
    
    isMinor = False
    if revisionXML.minor is not None:
        isMinor = True
    revision = {'revision.id': revisionID,'minor':isMinor}

    return revision

def distributeColumns(dictstream,index_name,ignore_list=set()):
    publishers= {}
    for d in dictstream:
        keys= d.keys()
        # print(keys)
        if not index_name in keys:
            print(index_name,'missing',d)
        else:
            for k in keys:
                if not (k == index_name or k in ignore_list):
                    if not k in publishers.keys():
                        #print('started publisher', k)
                        publishers[k]=list()
                    publishers[k].append((d[index_name], d[k]))
    #print (publishers.keys())
    return publishers

def streamBatches(stream,nitems):
    currentbatch=[]
    for item in stream:
        currentbatch.append(item)
        if not len(currentbatch)<nitems:
            yield currentbatch
            currentbatch=[]
    if currentbatch:
        yield currentbatch


def to_parquet(parquet_dir,fn,publisher,stream,blockid):
    #print('gathering parquet data',fn)
    pd= pandas.DataFrame.from_dict(dict(stream),
                                   orient='index',
                                   columns=[publisher])
#     print('dumping parquet data',parquet_dir,blockid,'--',publisher)
    df= ddf.from_pandas(pd,chunksize=1000)
#     print(os.path.join(parquet_dir,fn.replace('/','__') + 
#                                blockid +'.parquet_dir_'+publisher))
    df.to_parquet(os.path.join(parquet_dir,fn.replace('/','__') + 
                               blockid +'.parquet_dir_'+publisher),compression='brotli')
    return True 


def processChunk(filename,parquet_dir,nitems):
    batches=[]
    for batch in streamBatches(
                    streamWikipediaRevisions(filename),
                    nitems):
        dict_of_columns=distributeColumns(batch, 'revision.id')
        fileindex= dict(dict_of_columns['revision.fileindex']).values()
        batch_numbers = '--'.join([str(min(fileindex)),
                                   str(max(fileindex))])
        for colname, col in dict_of_columns.items():
            if colname == 'revision.fileindex':
                continue
            to_parquet(parquet_dir, filename, 
                       colname, col,
                       batch_numbers)

        batches.append(batch_numbers)
    return (filename,parquet_dir,batches)





def main():
    import argparse
    argparser= argparse.ArgumentParser('extract and store Wikipedia revision wikilinks')
    argparser.add_argument('infile')
    argparser.add_argument('parquetfile')
    args=argparser.parse_args()
    fn= args.infile
    parquetfn= args.parquetfile
    nitems= 2500
#     print(fn)
#     print(parquetfn)

    processChunk(fn,parquetfn,nitems)


if __name__ == '__main__':
    main()
