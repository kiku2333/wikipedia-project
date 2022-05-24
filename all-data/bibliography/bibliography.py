#!/usr/bin/python3

import dask.dataframe as ddf
import collections
import pandas as pd
import json
import os
import numpy as np
import re
from Levenshtein import distance
import mwparserfromhell
from difflib import SequenceMatcher
from glob import glob
from collections import ChainMap
import time
import argparse


argparser= argparse.ArgumentParser('find bibliography info')
argparser.add_argument('infile')
args=argparser.parse_args()

original_folder = args.infile
folder = original_folder + '/'

refs = ddf.read_parquet('/home/ubuntu/intermediate-result-new-select-editors/bibliography/refs-added-modified-removed/' + folder).compute()


path = '/home/ubuntu/enwiki-columns/'
all_folders = glob(path + '*')
    
page_titles = [f for f in all_folders if 'page.title' in f]
titles = ddf.read_parquet(page_titles).compute()
titles = titles.drop(columns={'dir0'})

editors_files = [f for f in all_folders if 'contributor.username' in f]
editor = ddf.read_parquet(editors_files).compute()
editor = editor.drop(columns={'dir0'})

page_ref_df = refs.join(titles).join(editor)


def find_tag_name(content):
    # remove special characters
    wikicode = mwparserfromhell.parse(content)
    tag = wikicode.filter_tags()
    if len(tag) == 0:
        # some how cannot parse properly
        sub_string = re.search(r'name=\"(.*?)\"', content)
        if sub_string is not None:
            return sub_string.group(1)
        else:
            return None
    attrs = tag[0].attributes
    tag_name = None
    for attr in attrs:
        if attr.name == 'name' and (attr.value):
            tag_name = attr.value.strip()
            tag_name = re.sub('[^A-Za-z0-9.]+', ' ', tag_name).strip()
    return tag_name


def similar(c1,c2):
    n = max(len(c1),len(c2))
    if n == 0:
        return 1
    return distance(c1.lower(),c2.lower())/n

def has_same_field(a,b,field):
    parse_a = parse_content(a)
    parse_b = parse_content(b)
    
    # both have id
    if (field in parse_a) and (field in parse_b):
        if (parse_a[field] == parse_b[field]) and (parse_a[field] is not None) and (parse_b[field] is not None):
            return True
        else:
            return False
    
#     # both no id
    if (field not in parse_a) and (field not in parse_b):
        return False
    
    search_id = ''
    if field not in parse_b:
        search_id = parse_a[field]
        if search_id is not None:
            return search_id in b.lower()
    if field not in parse_a:
        search_id = parse_b[field]
        if search_id is not None:
            return search_id in a.lower()
    return False

def parse_content(content):
    wikicode = mwparserfromhell.parse(content)
    templates = wikicode.filter_templates()
    ref_id = None
    data = {}
    if len(templates) != 0:
        cite_type = templates[0].name.strip()
        params = templates[0].params     
        if cite_type.lower() == 'cite pmid' or cite_type.lower() == 'cite pubmed':
            ref_id = 'pmid: ' + templates[0].params[0].strip()
        else:         
            for item in params:
                key = item.name.strip().lower()
                key = re.sub('[^A-Za-z0-9.]+', '', key)
                value = item.value.strip().replace('\\n',' ').strip().lower()
                value = re.sub('\[','',value)
                value = re.sub('\]','',value)
                
                data[key]= value.lower()
                
            if ('pmid' in data) and (data['pmid']!=''):
                ref_id = 'pmid: ' + data['pmid']
            elif ('doi' in data) and (data['doi'] != ''):
                ref_id = 'doi: ' + data['doi']
            elif ('isbn' in data) and (data['isbn'] != ''):
                ref_id = 'isbn: ' + data['isbn']
            elif ('id' in data) and (data['id'] != ''):
                ref_id = data['id']

        data['ref_id'] = ref_id

    else:
        # parsefromhell
        url = re.findall(r'https?://[^\s<>";\]]+|www\.[^\s<>";\]]+', content)
        if len(url) > 0:
            data['url'] = url[0]
        content = re.sub('\[','',content)
        content = re.sub('\]','',content)
        data['unparsed'] = content.lower()
    
    return data



def find_sim_pairs(a,b):
    matched_indexes = []
    similar_pairs = {}
    same_pairs = {}
    
    for i in range(len(a)):
        val_a = a[i]
        for j in range(len(b)):
            if j in matched_indexes:
                continue
            val_b = b[j]
            
            
            # if sim is 0, two strings are exactly the same, it should not be an added or modified or removed ref
            # this may caused by the format of the reference
            # then add to same_pairs instead
            # remove the same_pairs later, but do not add it to modified list
            if val_a == val_b:
                matched_indexes.append(j)
                same_pairs[val_a] = val_b
                break
            
            
            
            same_id = has_same_field(val_a,val_b,'ref_id')
            same_title = has_same_field(val_a,val_b,'title')
            same_author = has_same_field(val_a,val_b,'author')
            same_url= has_same_field(val_a,val_b,'url')
            sim = similar(val_a,val_b)
            
            
            if sim < 0.5 or same_id or same_title or same_author or same_url or (val_b in val_a):
                # use removed as key and added as value, to be same with modified computed before
#                 similar_pairs[val_a] = val_b
                similar_pairs[val_b] = val_a
                matched_indexes.append(j)
                # found a match, then break the for loop for j
                break
            # same ref name
            if find_tag_name(val_a) == find_tag_name(val_b):
#                     similar_pairs[val_a] = val_b
                similar_pairs[val_b] = val_a
                matched_indexes.append(j)
                break
                
            

    add_matched = [x for x in similar_pairs.values()]
    removed_matched = [x for x in similar_pairs.keys()]
    same_add = [x for x in same_pairs.keys()]
    same_removed = [x for x in same_pairs.values()]
    return [similar_pairs,list(set(a)^set(add_matched)^set(same_add)),list(set(b)^set(removed_matched)^set(same_removed))]

def update_modification(modified):
    if modified == []:
        return {}
    # flattern list of dicts
    modified = dict(ChainMap(*modified))
    return modified

def process_refs(d):
    added_res = d['refs.added']
    removed_res = d['refs.removed']
    modified = d['refs.modified']
    modified = update_modification(modified)
    # if one of added and removed is none, then do not need to compare and update the modification
    # just needto update modified
    if (added_res == []) | (removed_res == []):
        return [modified,added_res,removed_res]
    
#     added_res= split_refs(added)
#     removed_res= split_refs(removed)
    
    x = find_sim_pairs(added_res,removed_res)
    m = {}
    a = []
    r = []
    
    if x is not None:
        m = x[0]
        a = x[1]
        r = x[2]
    m.update(modified)
    return [m,a,r]

page_ref_df[['modified','added','removed']] = page_ref_df.apply(lambda d: pd.Series(process_refs(d)),axis=1)
fin_ref_amr = page_ref_df[['added','modified','removed']]
fin_ref_amr_df = ddf.from_pandas(fin_ref_amr,chunksize=10000)
fin_ref_amr_df.to_parquet('/home/ubuntu/intermediate-result-new-select-editors/bibliography/refs-added-modified-removed-final/' + folder, object_encoding='json')


from random import randint
self_created_id = []

def generated_id(title): 
    tmp_id = randint(100000, 999999)
    # if self generated id is already used, generate another one
    while (title + '.' + 'id.' + str(tmp_id) in self_created_id):
        tmp_id = randint(100000, 999999)
    ref_id = title + '.' + 'id.' + str(tmp_id)
    self_created_id.append(ref_id)
    return ref_id

def get_tag_content(ref):
    return re.search('<ref[^>]*>[^<]*',ref)[0].split('>')[1]
#     wikicode = mwparserfromhell.parse(ref)
#     tag = wikicode.filter_tags()
#     if len(tag) == 0:
#         return re.search(r'>(.*?)<', ref).group(1)
#     return tag[0].contents.strip()

def extract_ref_with_name(ref):
    tag_name = find_tag_name(ref)
    tag_content = get_tag_content(ref)
    return [tag_name,tag_content]

# ref_list: reference list, key: ref id, value: parsed ref content 
# res: result list, which ref is added by which user to which article
# matched_ids: matched id pairs, key: old id, value: new id 
# ref_id_pairs: key: unparsed ref content, value: ref id, use unparsed since parsed content is complex of being a key
ref_list = {}
res = []
matched_ids = {}
ref_id_pairs = {}


# add ref info to ref list
def insert_to_dicts(rid,content,title,user):
    # if content is already in ref list (use ref_id_pairs to check it)
    # use new id and replace old id
    if content in ref_id_pairs:
        old_id = ref_id_pairs[content]

        # second condition is used to prevent loops
        # if old is is already used as a key in matched_ids (matched other ids)
        # then do nothing
        if (old_id != rid) & (rid not in matched_ids):    
            matched_ids[old_id] = rid
            ref_id_pairs[content] = rid
            ref_list[rid] = parse_content(content)

            ref_list.pop(old_id, None)
            res.append({'ref_id':rid,'page.title':title,'contributor':user})
            all_vals = list(matched_ids.values())
            if old_id in all_vals:
                # get key and replace value
                # get list of keys, may have multiple keys
                keys = [k for k, v in matched_ids.items() if v == old_id]
                for key in keys:
                    if key != rid:
                        matched_ids[key] = rid

    # else a new ref, then use rid and add refs to both lists
    else:
        ref_list[rid] = parse_content(content)
        ref_id_pairs[content] = rid
        res.append({'ref_id':rid,'page.title':title,'contributor':user})

# parse the ref and add ref to ref list
# modified_id are used to process modified refs
def process_single_ref(ref,title,user,modified_id):
    [tag_name, tag_content] = extract_ref_with_name(ref)

    if tag_name is None:
        parsed = parse_content(tag_content)
        ref_id = ''
        if modified_id!='':
            ref_id = modified_id
        elif ('ref_id' in parsed) and (parsed['ref_id'] is not None):
            ref_id = parsed['ref_id']
        else:
            ref_id = generated_id(title)
        insert_to_dicts(ref_id,tag_content,title,user)
    else:
        # has a tag name
        ref_id = title + '.' + tag_name
        # if no content, add to result list
        if tag_content == '':
            res.append({'ref_id':ref_id ,'page.title':title,'contributor':user})
        else:
            parsed = parse_content(tag_content)
            # modified ref
            if modified_id != '':
                insert_to_dicts(modified_id,tag_content,title,user)
            # use title + tagName as ref id
            else:
                insert_to_dicts(ref_id,tag_content,title,user)

def process_added_refs(d):
    added_refs = d['added']
    for ref in added_refs:
        process_single_ref(ref,d['page.title'],d['contributor.username'],'')

a = page_ref_df.apply(lambda d: process_added_refs(d),axis=1)

def process_modified_refs(d):
    modified = d['modified']
    title = d['page.title']
    user = d['contributor.username']
    # the added refs should already in ref_list
    # just need to find proper ids and update the new info in ref list, ref id pairs, and page user ref info 
    if modified == {}:
        return
    for old, new in modified.items():
        # if old id in ref_id_pairs, use old id for the new ref
        if old in ref_id_pairs:
            ref_id = ref_id_pairs[old]
            process_single_ref(new,title,user,ref_id)
        # else not in ref_id_pairs, add as a new
        else:
            process_single_ref(new,title,user,'')
                    
b = page_ref_df.apply(lambda d: process_modified_refs(d),axis=1)


# 1. compare urls

x = pd.DataFrame.from_dict(ref_list,orient='index')

x = x.fillna('')

x['ref_id'] = x.index

def remove_url_prefix(url):
    t = re.compile(r"(https?://)?(www.)?")
    return t.sub('', url).strip().strip('/').lower()
x['url'] = x['url'].apply(remove_url_prefix)

def get_unique_index(index):
    return list(set(index))
url = x.groupby('url').agg({'ref_id':get_unique_index})

# drop the column with no url
url = url[url.index != '']
# all pairs with same url
same_urls_pairs = url['ref_id'].to_list()

def sim_pairs(pairs):
    sim_pairs = {}
    for item in pairs:
        if len(item) == 1:
            continue
        else:
            val_index = 0
            for i in range(0,len(item)):
                if item[i].startswith('pmid'):
                    val_index = i
                    break
                if item[i].startswith('doi'):
                    val_index = i
                    break
                if item[i].startswith('isbn'):
                    val_index = i
                    break
                # tag name that does not contain .id.
                if re.search('.id.', item[i]) == None:
                    val_index = i
                    break
            for i in range(0,len(item)):
                if i == val_index:
                    continue
                else:
                    sim_pairs[item[i]] = item[val_index]
                
    return sim_pairs

url_dict = sim_pairs(same_urls_pairs)


def update_res(drop_index,replace_list):
    for item in res:
        if item['ref_id'] in drop_index:
            item['ref_id'] = replace_list[item['ref_id']]

# update based on matched_ids from previous part
drop_index = list(matched_ids.keys())
update_res(drop_index,matched_ids)

# update url_dict to prevent loops 
url_dict = {k:v for k,v in url_dict.items() if v not in matched_ids}
drop_index = list(url_dict.keys())
update_res(drop_index,url_dict)

# add url_dict to matched_ids
matched_ids.update(url_dict)


# 2. For formatted refs, compare title, author, publisher, etc. 
formatted = x[x['unparsed'] == '']

# For each type, we need to look at the title, if tht titles are similar, then compare other criteria

def similar_fields(df,field,tol):
    possible_pairs = []
    field_list = df[field].to_list()
    df_index = df.index.to_list()
    exist_index = []
    for i in range(len(field_list)):
        curr_index = df_index[i]
        curr_val = field_list[i]
        if curr_index in exist_index:
            continue
        if len(curr_val) == 0:
            continue
        ratio = df.apply(lambda d: similar(d[field],curr_val),axis=1)
#         ratio = x/len(curr_title)
        tmp = []
        for i in range(len(ratio)):
            curr_r = ratio[i]
            if curr_r < tol:
                tmp.append(df_index[i])
                exist_index.append(df_index[i])
        if len(tmp) > 1:
            possible_pairs.append(tmp)
    return possible_pairs
from itertools import combinations

def compare_cols(df,possible_pairs,metric,tol):
    sim_pairs = []
    
    for p in possible_pairs:
        tem_pairs = []
        x = list(combinations(p,2))

        for item in x:
            d1 = df.loc[item[0]][metric]
#             d1 = re.sub('[^A-Za-z0-9]+', '', d1)
            

            d2 = df.loc[item[1]][metric]
#             d2 = re.sub('[^A-Za-z0-9]+', '', d2)
            

            change_rate = similar(d1,d2)

            if change_rate <= tol:
                tem_pairs.append(item[0])
                tem_pairs.append(item[1])
            
        pairs = list(set(tem_pairs))
        sim_pairs.append(pairs)
            
    return [x for x in sim_pairs if x]

possible_pairs = similar_fields(formatted,'title',0.3)
# compare some import parts
filter_publisher, filter_author, filter_journal, filter_work, filter_url = ([] for i in range(5))
if 'publisher' in formatted:
    filter_publisher = compare_cols(formatted,possible_pairs,'publisher',0.3) 
if 'author' in formatted:
    filter_author = compare_cols(formatted,possible_pairs,'author',0.3)
if 'journal' in formatted:    
    filter_journal = compare_cols(formatted,possible_pairs,'journal',0.3)
if 'work' in formatted:
    filter_work = compare_cols(formatted,possible_pairs,'work',0.3)
if 'url' in formatted:
    filter_url= compare_cols(formatted,possible_pairs,'url',0.1)
possible_pairs = list(set().union((tuple(row) for row in filter_publisher),(tuple(row) for row in filter_author),(tuple(row) for row in filter_journal),(tuple(row) for row in filter_work),(tuple(row) for row in filter_url)))
possible_pairs = [list(t) for t in possible_pairs]

replace_dict = sim_pairs(possible_pairs)
replace_dict = {k:v for k,v in replace_dict.items() if v not in matched_ids}
drop_index = list(replace_dict.keys())
update_res(drop_index,replace_dict)

matched_ids.update(replace_dict)


# 3. compare content for all unformatted try to find similar one. 
unformatted = x[x['unparsed'] != '']

possible_pairs = similar_fields(unformatted,'unparsed',0.5)
replace_dict = sim_pairs(possible_pairs)
replace_dict = {k:v for k,v in replace_dict.items() if v not in matched_ids}
drop_index = list(replace_dict.keys())
update_res(drop_index,replace_dict)
# unformatted = unformatted[~unformatted['ref_id'].isin(drop_index)]

matched_ids.update(replace_dict)

# 4. For unformatted refs, find if there is a match with formatted refs (search in unforamtted refs with existing 
#    ids/authors in formatted)
def find_match_with_format(content,exist_field):
    for i in exist_field.index:
        x = exist_field[i]
        if x.lower() in content.lower():
            return i
    return ''

def form_sim_pairs(id1,id2,l):
    l.append([id1,id2])
# matched pmid
# may not have pmid
if 'pmid' in formatted:
    exist_pmid = formatted[formatted['pmid']!='']['pmid']
    unformatted['matched_pmid'] = unformatted.apply(lambda d: find_match_with_format(d['unparsed'],exist_pmid),axis=1)
    matched_pmid_df = unformatted[unformatted['matched_pmid']!='']
    matched_pmid = []
    c = matched_pmid_df.apply(lambda d: form_sim_pairs(d['ref_id'],d['matched_pmid'],matched_pmid),axis=1)
    replace_dict = sim_pairs(matched_pmid)
    replace_dict = {k:v for k,v in replace_dict.items() if v not in matched_ids}
    matched_ids.update(replace_dict)
    drop_index = list(replace_dict.keys())
    update_res(drop_index,replace_dict)



# author
exist_author = formatted[formatted['author']!='']['author']
unformatted['matched_authors'] = unformatted.apply(lambda d: find_match_with_format(d['unparsed'],exist_author),axis=1)
matched_authors_df = unformatted[unformatted['matched_authors']!='']

matched_authors= []
d = matched_authors_df.apply(lambda d: form_sim_pairs(d['ref_id'],d['matched_authors'],matched_authors),axis=1)

replace_dict = sim_pairs(matched_authors)
replace_dict = {k:v for k,v in replace_dict.items() if v not in matched_ids}
matched_ids.update(replace_dict)
drop_index = list(replace_dict.keys())
update_res(drop_index,replace_dict)
# unformatted = unformatted[~unformatted['ref_id'].isin(drop_index)]

# isbn
if 'isbn' in formatted:
    exist_isbn = formatted[formatted['isbn']!='']['isbn']
    unformatted['matched_isbn'] = unformatted.apply(lambda d: find_match_with_format(d['unparsed'],exist_isbn),axis=1)

    matched_isbn_df = unformatted[unformatted['matched_isbn']!='']
    matched_isbn= []
    e = matched_isbn_df.apply(lambda d: form_sim_pairs(d['ref_id'],d['matched_isbn'],matched_isbn),axis=1)


    replace_dict = sim_pairs(matched_isbn)
    replace_dict = {k:v for k,v in replace_dict.items() if v not in matched_ids}
    matched_ids.update(replace_dict)
    drop_index = list(replace_dict.keys())
    update_res(drop_index,replace_dict)
    # unformatted = unformatted[~unformatted['ref_id'].isin(drop_index)]

# doi
if 'doi' in formatted:
    exist_doi = formatted[formatted['doi']!='']['doi']
    unformatted['matched_doi'] = unformatted.apply(lambda d: find_match_with_format(d['unparsed'],exist_doi),axis=1)

    matched_doi_df = unformatted[unformatted['matched_doi']!='']
    matched_doi= []
    f = matched_doi_df.apply(lambda d: form_sim_pairs(d['ref_id'],d['matched_doi'],matched_doi),axis=1)
    replace_dict = sim_pairs(matched_doi)
    replace_dict = {k:v for k,v in replace_dict.items() if v not in matched_ids}
    matched_ids.update(replace_dict)
    drop_index = list(replace_dict.keys())
    update_res(drop_index,replace_dict)
    # unformatted = unformatted[~unformatted['ref_id'].isin(drop_index)]


# final_ref_list = pd.concat([formatted,unformatted],sort=False)
final_ref_list = pd.DataFrame.from_dict(ref_list,orient='index')
res = pd.DataFrame(res)
final_ref_list = final_ref_list[final_ref_list.index.isin(res['ref_id'].unique())]
res_df = ddf.from_pandas(res,chunksize=10000)
final_ref_list_df = ddf.from_pandas(final_ref_list,chunksize=10000)

res_df.to_parquet('/home/ubuntu/intermediate-result-new-select-editors/bibliography/ref-page-user-info/' + folder)
final_ref_list_df.to_parquet('/home/ubuntu/intermediate-result-new-select-editors/bibliography/ref-list-info/' + folder)


def get_ref_former(added, modified, removed):
    return [len(added), len(modified),len(removed)]

page_ref_df[['ref_added','ref_modified','ref_removed']] = page_ref_df.apply(lambda d: pd.Series(get_ref_former(d['added'],d['modified'],d['removed'])),axis=1)
x = page_ref_df[['ref_added','ref_modified','ref_removed']]

x_df = ddf.from_pandas(x,chunksize=10000)

x_df.to_parquet('/home/ubuntu/intermediate-result-new-select-editors/editor-profile/ref-info/' + folder)
