from functools import reduce
from enum import Enum
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pm4py.algo.discovery.dfg.adapters.pandas import df_statistics
from pm4py.objects.log.util import dataframe_utils


def mine_dfg_metrics(log):
    log = dataframe_utils.convert_timestamp_columns_in_df(log, timest_columns=['time:timestamp'])
    dfg_frequency, dfg_performance = df_statistics.get_dfg_graph(log, measure="both",
                                                                    activity_key='concept:name',
                                                                    timestamp_key='time:timestamp',
                                                                    case_id_glue='case:concept:name',
                                                                    start_timestamp_key=None,
                                                                    # perf_aggregation_key="mean")
                                                                    perf_aggregation_key="all")
    # print(dfg_frequency)
    # print(dfg_performance)

    df_freq = pd.DataFrame([ [ key[0], key[1], value ] for key, value in dfg_frequency.items() ], columns=[ 'src', 'tgt', 'freq' ])
    df_perf = pd.DataFrame([ [ key[0], key[1], value['mean'], value['median'], value['min'], value['max'], value['stdev'] ] for key, value in dfg_performance.items() ], 
                        columns=[ 'src', 'tgt', 'time_mean', 'time_median', 'time_min', 'time_max', 'time_stdev' ])
    # df_perf = pd.DataFrame([ [ key[0], key[1], value ] for key, value in dfg_performance.items() ], 
    #                     columns=[ 'src', 'tgt', 'time_mean' ])

    # combine both metrics for each edge
    return df_freq.merge(df_perf, how='inner', on=['src', 'tgt'], validate=None)


class GroupTypes(Enum):
    BY_ELEMENT = 'by_element'
    BY_LOG = 'by_log'

def compare_logs_dfg(logs):
    dfgs = []
    for index, log in enumerate(logs):
        dfg = mine_dfg_metrics(log)
        dfg.columns = [ f"{col}_{index}" if (col != 'src' and col != 'tgt') else col for col in dfg.columns ]
        dfgs.append(dfg)

    # outer join over all DFG dataframes on edges
    # if DFG has edge, will have value copied for metric columns
    # if DFG does not have edge, will have NaN for metric columns
    # ('suffixes' somehow not working propertly for multiple merges)
    merge = reduce(lambda dfg_x, dfg_y: dfg_x.merge(dfg_y, on=[ 'src', 'tgt' ], how='outer', suffixes=("", "")), dfgs)
    # print(merge)

    return [ dfgs, merge ]


def compare_nodes_edges_dfg(logs):
    # get DFGs for each log & their outer join
    dfgs, merge = compare_logs_dfg(logs)

    node_diff = []
    edge_diff = []
    
    # compare each DFG to every other DFG
    for i in range(len(dfgs)):
        dfg_i = dfgs[i]

        # get unique nodes in DFG i
        unique_i = set([*dfg_i['src'], *dfg_i['tgt']])
        
        for j in range(len(dfgs)):
            if i == j:
                continue

            dfg_j = dfgs[j]
            # get unique nodes in DFG j
            unique_j = set([*dfg_j['src'], *dfg_j['tgt']])

            # compare two node sets
            only_in_i = (unique_i - unique_j)
            node_diff.extend([ [ i, j, node ] for node in only_in_i ])

            # get edges found in DFG i but not in DFG j
            only_in_i = merge.loc[pd.isnull(merge[f'freq_{j}'])&pd.notnull(merge[f'freq_{i}']), [ 'src', 'tgt' ]].to_numpy()
            edge_diff.extend([ [ i, j, edge[0], edge[1] ] for edge in only_in_i ])

    # for DFG i, keeps unique/missing nodes/edges per DFG j
    node_diff = pd.DataFrame(node_diff, columns=[ 'dfg_i', 'dfg_j', 'node' ])
    edge_diff = pd.DataFrame(edge_diff, columns=[ 'dfg_i', 'dfg_j', 'src', 'tgt' ])

    return [ node_diff, edge_diff ]


def print_nodes_edges_cmp(logs, cmp_results, log_labels=None, group_type=GroupTypes.BY_ELEMENT):
    node_diff, edge_diff = cmp_results

    label_fn = lambda nr: log_labels[nr] if log_labels is not None else str(nr)

    if group_type == GroupTypes.BY_ELEMENT:

        list_fn = lambda s: ("log(s) " if log_labels is None else "") + ', '.join(map(label_fn, s.unique()))
        for v, g in node_diff.groupby(['node']):
            print(f". event '{v[0]}' from {list_fn(g['dfg_i'])} not found in {list_fn(g['dfg_j'])}")

        for v, g in edge_diff.groupby(['src', 'tgt']):
            print(f". edge '{v[0]} -> {v[1]}' from {list_fn(g['dfg_i'])} not found in {list_fn(g['dfg_j'])}")

        print()

    elif group_type == GroupTypes.BY_LOG:
        # for each DFG j
        for i in range(len(logs)):
            print(f"> log {label_fn(i)}")

            # for each DFG j
            for j in range(len(logs)):
                if i == j:
                    continue

                print(f"- compared to log {label_fn(j)}:")

                # nodes found in i but not j
                extra_nodes = node_diff.loc[(node_diff['dfg_i']==i)&(node_diff['dfg_j']==j),'node']
                if len(extra_nodes) > 0:
                    print(". extra events:", ', '.join(extra_nodes))

                # nodes found in j but not i
                missing_nodes = node_diff.loc[(node_diff['dfg_i']==j)&(node_diff['dfg_j']==i),'node']
                if len(missing_nodes) > 0:
                    print(". missing events:", ', '.join(missing_nodes))

                # edges found in i but not j
                extra_edges = edge_diff.loc[(edge_diff['dfg_i']==i)&(edge_diff['dfg_j']==j), [ 'src', 'tgt' ]].to_numpy()
                if len(extra_edges) > 0:
                    print(". extra edges:\n", "\n".join([ f"{r[0]} -> {r[1]}" for r in extra_edges ]))

                # edges found in j but not i
                missing_edges = edge_diff.loc[(edge_diff['dfg_i']==j)&(edge_diff['dfg_j']==i), [ 'src', 'tgt' ]].to_numpy()
                if len(missing_edges) > 0:
                    print(". missing edges:\n", "\n".join([ f"{r[0]} -> {r[1]}" for r in missing_edges ]))

                if len(extra_nodes) == 0 and len(missing_nodes) == 0 and len(extra_edges) == 0 and len(missing_edges) == 0:
                    print("no differences")
                print()
            print()


# inspired by pm4py.util.vis_utils
def human_readable_unit(time):
    units = [ [ 'min', 60 ], [ 'hr', 60 ], [ 'day', 24 ], [ 'month', 30 ], [ 'year', 365 ] ]
    ret = { 'unit': 'sec', 'divis': 1 }
    for unit, divis in units:
        time = time // divis
        if time == 0:
            return ret
        ret['unit'] = unit
        ret['divis'] = ret['divis'] * divis

# TODO generalize this code
def plot_metrics_dfg(logs, log_labels=None, metric1=None, metric2=None, normalize=None, time_unit=None, per_edge=False, edges=None):
    _, axes = plt.subplots(nrows=1, ncols=2, figsize=(12, 5))
    plot_metric_dfg(logs, log_labels, metric1, normalize, time_unit, per_edge, edges, subplot_of=axes[0])
    plot_metric_dfg(logs, log_labels, metric2, normalize, time_unit, per_edge, edges, subplot_of=axes[1])

# normalize: True/False
# metric: 'freq', 'time_mean', 'time_median', 'time_min', 'time_max', 'time_stdev'
# time_unit: min, hr, day, month, year
def plot_metric_dfg(logs, log_labels=None, metric='freq', normalize=None, time_unit=None, per_edge=False, edges=None, subplot_of=None):
    _, merge = compare_logs_dfg(logs)

    # filter non-metric columns
    merge = merge[['src', 'tgt', *[ f'{metric}_{i}' for i in range(len(logs)) ]]]
    # change metric column names to log id
    # (default to "log <nr>" if log_labels not given)
    label_fn = lambda nr: log_labels[int(nr)] if log_labels is not None else f"log {nr}"
    # expected: <metric>_<i> where <i> indicates number of DFG
    merge.columns = [ f"{label_fn(col[col.rfind('_')+1:])}" if "_" in col else col for col in merge.columns ]
    
    # add "edge" column
    merge['edge'] = merge['src'] + " -> " + merge['tgt']
    # all metric-related columns
    metric_cols = ~merge.columns.isin([ 'src', 'tgt', 'edge' ])
    
    # name Y axis after metric
    ylabel = metric

    normalize_time = False
    if normalize is not None: 
        if metric == 'freq':
            ylabel += " (%)"
            # value = percentage of the frequency towards the sum of all frequencies
            merge.loc[:,metric_cols] = merge.loc[:,metric_cols] / merge.loc[:,metric_cols].sum() * 100
        else:
            normalize_time = True

    def do_normalize_time(sel):
        nonlocal metric_cols, time_unit, ylabel

         # get all time values (not incl. nan's)
        tmp_vals = sel.loc[:,metric_cols].to_numpy()
        tmp_vals = tmp_vals[~np.isnan(tmp_vals)]
        # get most suitable unit
        unit = time_unit if time_unit is not None else human_readable_unit(tmp_vals.max())
        ylabel = unit['unit']
        # normalize values to unit
        sel.loc[:,metric_cols] = sel.loc[:,metric_cols] / unit['divis']

    def do_plot(df, title, ylabel, x=None, legend=True):
        if subplot_of is not None:
            df.plot.bar(ylabel=ylabel, x=x, legend=legend, subplots=True, ax=subplot_of)
            subplot_of.title.set_text(title)
        else:
            df.plot.bar(title=title, ylabel=ylabel, x=x, legend=legend)

    def plot_selection(sel, title):
        nonlocal normalize_time, metric_cols, ylabel

        if len(sel) == 0:
            return
        if normalize_time:
            do_normalize_time(sel)
        # transpose - need the dataframe topsy-turvy!
        sel = sel.loc[:,metric_cols].transpose()
        do_plot(sel, title, ylabel, x=None, legend=False)

    if per_edge:
        title = f"{metric} - "
        if edges is not None:
            for edge in edges:
                # select subset for given edge
                sel = merge.loc[(merge['src']==edge[0])&(merge['tgt']==edge[1]),:]
                plot_selection(sel, title + f"{edge[0]} -> {edge[1]}")
        else:
            for v, g in merge.groupby(['edge']):
                plot_selection(g, title + v[0])
    else:
        if normalize_time:
            do_normalize_time(merge)
        do_plot(merge, metric, ylabel, x='edge', legend=True)