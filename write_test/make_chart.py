import json
import math
from plotly import tools
import plotly.plotly as py
import plotly.graph_objs as go


def parse_file(filename):
    seqs = {}
    with open(filename) as fh:
        for line in fh:
            if line[0] != '>':
                continue
            line_data = json.loads(line[1:])
            for k, v in line_data.items():
                if k not in seqs:
                    seqs[k] = {'mib': [], 'mib_s': []}
                for item, val in v.items():
                    seqs[k][item].append(val)
    return seqs


COLOR_MAP = {'postgres': 'orange', 'c_lmdb': 'green', 'lmdb': 'blue', 'sqlite': 'purple'}


def make_subplot(data):
    traces = []
    annotations = []
    for k in sorted(data, key=lambda x: x[4:] if x.startswith('syn_') else x):
        if k.endswith(' cum'):
            continue
        v = data[k]
        is_syn = k.startswith('syn_')
        color = COLOR_MAP[k[4:] if is_syn else k]
        traces.append(go.Scatter(x=v['mib'],
                                 y=v['mib_s'],
                                 name=k,
                                 legendgroup=k,
                                 yaxis='MiB/s',
                                 xaxis='MiB written',
                                 line={'width': (4 if is_syn else 0.5), 'color': color},
                                 mode='lines+markers' if is_syn else 'lines',
                                 marker=dict(size=(8 if is_syn else 0))
                                 ))
        last_y = v['mib_s'][-1]
        print(last_y)
        annotations.append(dict(xref='paper', yref='y', x=0.95, y=math.log10(last_y), text='{} MiB/s'.format(last_y),
                                xanchor='left', yanchor='middle',
                                showarrow=False))
    print(annotations)
    return traces


def draw_chart(traces1, traces2):
    layout = go.Layout()
    fig = tools.make_subplots(rows=2, cols=1, subplot_titles=('512MiB RAM, writing 1024MiB',
                                                              '2048MiB RAM, writing 8192MiB'))
    for trace in traces1:
        fig.append_trace(trace, 1, 1)
    for trace in traces2:
        trace['showlegend'] = False
        fig.append_trace(trace, 2, 1)
    fig['layout']['xaxis1'].update(title='MiB written', dtick=128)
    fig['layout']['yaxis1'].update(title='MiB/s', type='log', autorange=True)
    fig['layout']['xaxis2'].update(title='MiB written', dtick=512)
    fig['layout']['yaxis2'].update(title='MiB/s', type='log', autorange=True)
    fig['layout']['title'] = 'Write performance w.r.t. system RAM'
    py.plot(fig, layout=layout)


def main():
    import sys
    import argparse
    parser = argparse.ArgumentParser(description="Make a pretty chart")
    parser.add_argument("results_filenames", nargs='+')
    args = parser.parse_args()
    if len(args.results_filenames) != 2:
        print('Sorry, this currently takes exactly 2 filenames.')
        sys.exit(1)
    subplots = []
    for filename in args.results_filenames:
        data = parse_file(filename)
        subplots.append(make_subplot(data))
    draw_chart(*subplots)


if __name__ == '__main__':
    main()

