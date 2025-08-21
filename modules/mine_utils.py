import pm4py

from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.importer.xes import importer as xes_importer

# process mining 
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.algo.filtering.dfg.dfg_filtering import clean_dfg_based_on_noise_thresh

# viz
# (wvw: updated, courtesy https://stackoverflow.com/questions/75424412/no-module-named-pm4py-objects-petri-in-pm4py)
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.visualization.petri_net import visualizer as pn_visualizer
# (wvw: added)
from pm4py.visualization.dfg import visualizer as dfg_visualizer
from pm4py.visualization.heuristics_net import visualizer as hn_visualizer
from pm4py.visualization.process_tree import visualizer as pt_visualizer

# misc 
from pm4py.objects.conversion.process_tree import converter as pt_converter

# custom code
from .abstract_events import aggregate_events, generalize_events

import numpy as np
import pandas as pd
import os

def read_log(path):
    log = pd.read_csv(path)
    log['case:concept:name'] = log['case:concept:name'].astype('string')
    log['time:timestamp'] = pd.to_datetime(log['time:timestamp'])
    return log

def read_sub_log(name, select, path):
    log = pd.read_csv(os.path.join(path, "event logs", name))
    select_cases = pd.read_csv(os.path.join(path, select))
    log = log.loc[log['case:concept:name'].isin(select_cases['UniqueId']),]
    log['case:concept:name'] = log['case:concept:name'].astype('string')
    log['time:timestamp'] = pd.to_datetime(log['time:timestamp'])
    return log

# (got idea from clean_dfg_based_on_noise_thresh)
def clean_dfg_infreq_edges(dfg, geq):
    new_dfg = None
    for el in dfg:
        num = dfg[el]
        if num >= geq:
            if new_dfg is None:
                new_dfg = {}
            new_dfg[el] = dfg[el]
            
    return new_dfg

def mine_dfg(log, noise_threshold=0, edge_freq=1):
    dfg = dfg_discovery.apply(log)
    
    all_activ = log['concept:name'].unique()
    dfg = clean_dfg_based_on_noise_thresh(dfg, all_activ, noise_threshold)
    
    if edge_freq > 1:
        dfg = clean_dfg_infreq_edges(dfg, edge_freq)
    
    gviz = dfg_visualizer.apply(dfg, log=log, 
#                                variant=dfg_visualizer.Variants.PERFORMANCE, 
#                                parameters={ 'pm4py:param:start_timestamp_key': 'time:timestamp',
#                                             'pm4py:param:timestamp_key': 'time:timestamp'}
#                                             'pm4py:param:timestamp_key': 'end_timestamp' }
                                variant=dfg_visualizer.Variants.FREQUENCY 
    )
    dfg_visualizer.view(gviz)
    
    return dfg

def mine_pnet_alpha(log):
    net, initial_marking, final_marking = alpha_miner.apply(log)
    gviz = pn_visualizer.apply(net, initial_marking, final_marking)
    pn_visualizer.view(gviz)
    
def mine_pnet_induct(log, noise_threshold=0.2):
    net,initial_marking, final_marking = pm4py.discover_petri_net_inductive(log, noise_threshold=noise_threshold)
    gviz = pn_visualizer.apply(net, initial_marking, final_marking) 
    pn_visualizer.view(gviz)
    
def mine_pnet_heur(log):
    net, initial_marking, final_marking = heuristics_miner.apply(log)
    gviz = pn_visualizer.apply(net, initial_marking, final_marking)
    pn_visualizer.view(gviz)
    

def print_stat(stat_hrs):
    return stat_hrs/24

hr_to_day = lambda x: x/24

def show_stats(label, sr):
    if len(sr) == 0:
        return "<empty>"
    
    print(label)
    print(sr.describe())
    print()
#     descr = df.describe()
#     print(label, "median:", print_stat(descr['50%']), "mean:", print_stat(descr['mean']), "std:", print_stat(descr['std']))
    
def plot_stats(label, df):
    df = df.sort_values(by='Duration')
    _ = df.plot(y='Duration', x='case:concept:name', title=label)

def show_case_durations(log):
    print("per case, total length of stay at type of facility")
    print()
    
    hlog = log.copy()
    hlog['Duration'] = hlog['Duration'].map(hr_to_day)
    
    label = "emergency / acute"
    acute_dur = hlog.loc[hlog['TypeOfCare']==label,].groupby(['case:concept:name']).apply(lambda g: g['Duration'].sum())
    acute_dur = pd.DataFrame({ 'Duration': acute_dur, 'case:concept:name': acute_dur.index })
    plot_stats(label, acute_dur)
    show_stats(label, acute_dur)
    
    label = "rehab"
    rehab_dur = hlog.loc[hlog['TypeOfCare']==label,].groupby(['case:concept:name']).apply(lambda g: g['Duration'].sum())
    rehab_dur = pd.DataFrame({ 'Duration': rehab_dur, 'case:concept:name': rehab_dur.index })
    plot_stats(label, rehab_dur)
    show_stats(label, rehab_dur)
    
def show_facil_durations(log):    
    print("per type of facility, length of stay across all cases")
    print()
    
    hlog = log.copy()
    hlog['Duration'] = hlog['Duration'].map(hr_to_day)
    
    label = "emergency / acute"
    acute_dur = hlog.loc[(hlog['concept:name']==label),['case:concept:name', 'Duration']].reset_index(drop=True)
    plot_stats(label, acute_dur)
    show_stats(label, acute_dur)
    
    label = "rehab"
    rehab_dur = hlog.loc[(hlog['concept:name']==label),['case:concept:name', 'Duration']].reset_index(drop=True)
    plot_stats(label, rehab_dur)
    show_stats(label, rehab_dur)
    
#     dur_df = pd.DataFrame({ 'acute': acute_dur, 'rehab': rehab_dur })
#     plot_stats("Duration", dur_df)
    
def merge_files(to_merge, target):
    merged = pd.concat(map(pd.read_csv, to_merge), ignore_index=True)
    merged.to_csv(target, index=False)