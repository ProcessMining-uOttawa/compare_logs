import networkx as nx

# courtesy
# https://stackoverflow.com/questions/43108481/maximum-common-subgraph-in-a-directed-graph
def max_common_subgraph(g1, g2):
    matching_graph = nx.Graph()

    for n1,n2 in g2.edges():
        if g1.has_edge(n1, n2):
            matching_graph.add_edge(n1, n2)

    components = nx.connected_components(matching_graph)
    largest_component = max(components, key=len)
    
    return nx.induced_subgraph(g1, largest_component)


# - testing

# g1 = nx.DiGraph()
# g1.add_edge(1, 2)
# g1.add_edge(2, 3)
# g1.add_edge(2, 4)
# g1.add_edge(2, 5)
# nx.draw(g1, with_labels=True, font_color='w', pos=nx.spring_layout(g1))

# g2 = nx.DiGraph()
# g2.add_edge(1, 2)
# g2.add_edge(2, 3)
# g2.add_edge(2, 4)
# g2.add_edge(1, 5)
# nx.draw(g2, with_labels=True, font_color='w', pos=nx.spring_layout(g2))

# gc = max_common_subgraph(g1, g2)
# nx.draw(gc, with_labels=True, font_color='w')