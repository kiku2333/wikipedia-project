#!/usr/bin/python3
import dask.dataframe as ddf
import pandas as pd
import json
import os
import numpy as np
import hashlib
import re
from sklearn.metrics.pairwise import cosine_similarity
import networkx as nx
from infomap import Infomap
from math import pi


editor_profile = pd.read_parquet('../bibliography/result/bibliography.parquet')

all_users = list(editor_profile.columns)

res = {}

np.fill_diagonal(editor_profile.values,0)

def revert_list(user):
    res[user] = editor_profile[user].to_list()

result = [revert_list(i) for i in all_users]

num_users = len(all_users)

sim_matrix = np.empty((num_users,num_users))
# np.fill_diagonal(sim_matrix,np.nan)
np.fill_diagonal(sim_matrix,1)



for i in range(num_users - 1):
    for j in range(i + 1, num_users):
        u1 = all_users[i]
        u2 = all_users[j]
        sim = cosine_similarity([res[u1]],[res[u2]])
        sim_matrix[i][j] = sim
        sim_matrix[j][i] = sim



all_data_sim = pd.DataFrame(sim_matrix,index=all_users,columns=all_users)

all_data_sim.to_parquet('result/sim-matrix-based-on-biblio-coupling')
x = pd.DataFrame(all_data_sim.reset_index().melt('index').loc[lambda x : x['value']> 0].groupby('variable')['value'].apply(list))

x['len'] = x['value'].apply(len)

x = x[x['len']!=1]

all_data_sim = all_data_sim[all_data_sim.index.isin(x.index)]

all_data_sim = all_data_sim[all_data_sim.columns & x.index]

sim_matrix = all_data_sim



sim = sim_matrix.values

sim = sim - 0.01

editor_info = pd.DataFrame(all_users).reset_index().rename(columns={'index':'name',0:'contributor.username'})

res = pd.DataFrame()

def param_test(threshold, method):
    if method == 'sin':
        dis = np.arccos(sim)/pi
    else:
        dis = 1 - sim
    G = nx.from_numpy_matrix(dis) 
    edge_weights = nx.get_edge_attributes(G,'weight')
    G.remove_edges_from((e for e, w in edge_weights.items() if w > threshold))
    im = Infomap(silent=True, num_trials=100,include_self_links=False)   
    mapping = im.add_networkx_graph(G)
    im.run()
    df = im.get_dataframe(columns=["name","node_id", "module_id"])
    num_modules = len(df['module_id'].unique())
    a = df.groupby('module_id').count()
    num_singe_editor_groups = len(a[a['name'] == 1])
    max_ = a['name'].max()
    min_ = a['name'].min()
    greater_than_50 = len(a[a['name']>50])
    ans = 'threshold is {}, number of groups is {}, number of single editor groups is {}, '
    ans += 'max group contains {} editors, {} groups contains more than 50 editors'
    print(ans.format(threshold,num_modules,num_singe_editor_groups,max_,greater_than_50))
    
    fin = df.merge(editor_info,on='name')
    fin = fin.rename(columns={'module_id':'group'}).set_index('contributor.username')[['group']]
    res['threshold-' + str(threshold)] = fin['group']

test = np.arange(0,1.1,0.1).round(2)

for i in test:
    b = param_test(i,'original')


res.to_csv('result/cluster-param-test-by-biblio-coupling.csv',encoding='utf-16', sep="\t")