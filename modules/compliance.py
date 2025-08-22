from copy import deepcopy
from modules.gviz_utils import EditableDiGraph
from modules.variant_utils import Variant, get_variants_stats
from modules.align_log import align_bpmn_trace, pretty_print_alignments
from ipywidgets import Box, Layout, Select, Output, Label, HTML
from IPython.display import Image

import pm4py
from pm4py.visualization.bpmn import visualizer as bpmn_visualizer
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.visualization.dfg import visualizer as dfg_visualizer
from modules.mine_utils import read_log, mine_dfg

def show_text(output, txt, replace=True):
    with output:
        if replace:
            output.clear_output()
        output.append_stdout(txt)


def show_gviz(output, gviz, replace=True):
    with output:
        if replace:
            output.clear_output()
        output.append_display_data(Image(gviz.render()))

def compliance_bpmn_log(bpmn_path, log_path):
    log = read_log(log_path)
    var_stats = get_variants_stats(log)
    var_list = [Variant(index, row['cov_amt'], row['cov_perc'], row['cov_perc_cumul'],
                        row['sequence']) for index, row in var_stats.iterrows()]
    cur_sel = var_list[0]

    bpmn = pm4py.read_bpmn(bpmn_path)
    bpmn_gviz = bpmn_visualizer.apply(bpmn)
    bpmn_graph = EditableDiGraph(bpmn_gviz)

    var_label = HTML("<h2>Variant</h2>")
    var_model = Output()
    var_model_box = Box(children=[var_model], layout=Layout(overflow='scroll hidden'))
    var_sel = Select(options=var_list, value=cur_sel, disabled=False) # rows=10, description='variant'
    var_sel_box = Box(children=[var_sel], layout=Layout(max_width='175px'))
    var_box = Box(children=[var_sel_box, var_model_box], layout=Layout(display='flex', flex_flow='row'))
    align_label = HTML("<h2>Alignment</h2>")
    align_model = Output()
    align_box = Box(children=[align_model], layout=Layout(overflow='scroll hidden'))
    proc_label = HTML("<h2>Normative model</h2>")
    proc_model = Output()
    box = Box(children=[var_label, var_box, align_label, align_box, proc_label, proc_model], layout=Layout(display='flex', flex_flow='column'))

    # box = Box(children=[model_box, var_sel], layout=Layout(display='flex', flex_flow='row'))

    def on_var_sel(change):
        sel_var = change['new']

        show_text(var_model, sel_var.pretty_print())

        alignment = align_bpmn_trace(
            bpmn, sel_var.to_trace(), filter_invis=True)
        show_text(align_model, pretty_print_alignments(alignment))

        miss_activs = []
        for step in alignment['alignment']:
            if step[0] == ">>":  # missing in trace
                miss_activs.append(step[1])

        if len(miss_activs) > 0:
            new_graph = deepcopy(bpmn_graph)
            for miss_activ in miss_activs:
                new_graph.label_node[miss_activ]['color'] = 'lightpink'
                new_graph.label_node[miss_activ]['style'] = 'filled'
            show_gviz(proc_model, new_graph.to_gviz())

    var_sel.observe(on_var_sel, names='value')

    on_var_sel({'new': cur_sel})

    return box
