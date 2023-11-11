def print_by(col, df):
    groups = df.groupby(col)
    for group in groups:
        print(group)

def filter_evt_attr(log):
    return log.loc[:, [ 'case:concept:name', 'concept:name', 'time:timestamp' ]]



def aggregate_events(main_evt, sub_evts, log, debug=False):
    use_log = filter_evt_attr(log) if debug else log

    grouped = use_log.groupby('case:concept:name')
    for _, df in grouped:
        if debug:
            print(f"before: {df}")

        match_rows = df.loc[df['concept:name'].isin(sub_evts),:]
        sorted_rows = match_rows.sort_values(by='time:timestamp')

        update_log = df if debug else use_log
        update_log.loc[sorted_rows[0:].index, 'concept:name'] = main_evt
        update_log.drop(sorted_rows[1:].index, inplace=True)

        if debug:
            print(f"after: {df}\n\n")



def generalize_events(main_evt, sub_evts, log, debug=False):
    use_log = filter_evt_attr(log) if debug else log
    
    if debug:
        grouped = use_log.groupby('case:concept:name')
        for _, df in grouped:
            print(f"before: {df}")
            generalize_events(main_evt, sub_evts, df, False)
            print(f"after: {df}\n\n")
    else:
        use_log.loc[use_log['concept:name'].isin(sub_evts), 'concept:name'] = main_evt