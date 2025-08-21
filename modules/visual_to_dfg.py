def find_node_edges_rec(cur_src, cur_node, node_test, terms, graph, collect_edges, visited):
    """
    Recursively traverse a visual (e.g., lucidchart) graph and collect all edges that (indirectly) connect nodes that pass node_test.
    Used for when such nodes are indirectly connected through nodes _not_ passing node_test.
    Essentially, we only need the input graph to have "lines" with a "source" and "destination".

    Parameters
    -----------------
    cur_src
        current node of type node_type
    cur_node
        current node (possibly not of node_type)
    node_test
        function collects edges between nodes that pass node_test
    graph
        source graph representing the visual graph
    collect_edges
        collected edges between nodes that pass node_test
    visited
        list of id's of all visited nodes (that pass node_test)

    Returns
    -----------------
    """

    g_id, g_src, g_dst, g_label = terms['id'], terms['src'], terms['dst'], terms['label']

    # avoid infinite loops
    if node_test(cur_node):
        if cur_node[g_id] in visited:
            return
        visited.append(cur_node[g_id])

        # if node passes node_test, then consider it cur_src
        # need this at start; initial cur_node may (or not) pass node_test
        cur_src = cur_node

    # print("cur", ("<none>" if cur_src is None else cur_src['Id']), cur_node['Id'], cur_node['Name'], "'" + str(cur_node['Text Area 1']) + "'")

    # get all outgoing edges of cur_node
    out_edges = graph.loc[ graph[g_src]==cur_node[g_id], g_dst ]
    for _, out_edge_id in out_edges.items():
        # get destination node of outgoing edge
        node_dst = graph.loc[ graph[g_id]==out_edge_id,].iloc[0] # only 1 row; select it here

        # print("out", node_dst['Id'], node_dst['Name'], "'" + str(node_dst['Text Area 1']) + "'")

        # does destination node pass node_test?
        if node_test(node_dst):
            # if we already found a node passing node_test (cur_src), add edge between them
            if cur_src is not None:
                collect_edges[ ( str(cur_src[g_label]), str(node_dst[g_label]) ) ] = 1
        # if not, keep looking

        find_node_edges_rec(cur_src, node_dst, node_test, terms, graph, collect_edges, visited)


def lucid_to_dfg(graph, start_label):
    terms = {
        'id': 'Id',
        'label': 'Text Area 1',
        'src': 'Line Source',
        'dst': 'Line Destination',
    }

    node_ids = [ ['Terminator', None], ['Start Event', "End"] ]
    def node_test(node):
        for node_id in node_ids:
            if node['Name']==node_id[0] and (node_id[1] is None or node['Text Area 1']==node_id[1]):
                return True
        return False 

    dfg_dict = {}
    root = graph.loc[ graph[terms['label']]==start_label, ].iloc[0]
    find_node_edges_rec(None, root, node_test, terms, graph, dfg_dict, [])

    return dfg_dict