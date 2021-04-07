import plotly as py
import pandas as pd
import numpy as np
import json
import plotly.graph_objs as go
import plotly.express as px


# def create_plot(data=[], label="plot", label_x="x", label_y="y"):

#     x = []
#     y = []

#     for row in data:
#         y.append(row["numActiveCars"])
#         x.append(row["timestamp"])

#     df = pd.DataFrame({label_x: x, label_y: y})
#     df[label_x] = pd.to_datetime(df[label_x], unit='ms').tolist()

#     layout = go.Layout(title=label)

#     fig = px.line(df, x=label_x, y=label_y)

#     graphJSON = json.dumps(fig, cls=py.utils.PlotlyJSONEncoder)
#     #graphJSON = construct_json_graph(x, y, "timestamp", "cars", "scatter", {
#                                     #  "title": "Number of active cars"})
#     #print(graphJSON)
#     return graphJSON


def create_json_graph(x, y, chart_type="bar", layout={}):

    if chart_type == "scattergeo":
        fig = {
            "data": [{
                "lat": x,
                "lon": y,
                "type": chart_type,
            }], 
            "layout": layout,
            "title": layout.get("title","")}
            
    else:
        fig = {
            "data": [{
                "x": x,
                "y": y,
                "type": chart_type,
            }], 
            "layout": layout}

    return json.dumps(fig, cls=py.utils.PlotlyJSONEncoder)


def get_graph_params(json_graph):
    """Reads params from json graph

    Args:
        json_graph (dict): graph information

    Returns:
        params
    """

    x = json_graph.get("x", [])
    y = json_graph.get("y", [])
    chart_type = json_graph.get("type", "bar")
    layout = json_graph.get("layout", {})

    return x, y, chart_type, layout

    # TODO Parse validity?