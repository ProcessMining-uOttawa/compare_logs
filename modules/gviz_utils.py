import re
import graphviz

class EditableDiGraph:
    
    def __init__(self, gviz_ref):
        self.edges = []
        self.nodes = []
        self.label_node = {}
        
        for line in gviz_ref.body:
            line = line.strip()
            if match := re.search((r"(.*?) -> (.*)"), line):
                self.edges.append((match.group(1), match.group(2)))
            elif match := re.search(r"(.*) \[(.*)\]", line):
                node_id = match.group(1)
                props = match.group(2)
                matches = re.findall(r"([^\"\s]*?)\=([^\"\s]*)", props) + re.findall(r"([^\"\s]*?)\=\"([^\"]*)\"", props)
                props = { match[0]: match[1] for match in matches }
                props['__id'] = node_id
                
                label = props['label']
                if label == '': # (BPMN)
                    label = '__end' if props['fillcolor'] == 'orange' else '__start'
                
                self.nodes.append(props)
                self.label_node[label] = props
        
        # print(self.edges)
        # print(self.nodes)
        
    def to_dot(self):
        dot = "digraph {\n\n"
        dot += "\tgraph [bgcolor=white rankdir=LR]\n\n"
        
        for props in self.nodes:
            ssv = " ".join([ f"{key}={value}" for key, value in props.items() if not key.startswith("__") and key != "label" ])
            ssv = f"label=\"{props['label']}\" " + ssv
            dot += f"\t{props['__id']} [{ssv}]\n\n"
        
        dot += "\t"
        dot += "\n\n\t".join([ f"{edge[0]} -> {edge[1]}" for edge in self.edges ])
        dot += "\n\n\toverlap=false\n}"
            
        return dot
    
    def to_gviz(self):
        return graphviz.Source(self.to_dot(), format='png')

# test        
# bpmn = pm4py.read_bpmn("data/norm.bpmn")
# bpmn_gviz = bpmn_visualizer.apply(bpmn)

# dg = DiGraph(bpmn_gviz.body)
# dot = dg.to_dot()
# # print(dot)
# Source(dot)