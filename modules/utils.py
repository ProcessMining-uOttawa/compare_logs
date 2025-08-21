import ast

def read_dict(path):
    with open(path) as file:
        data = file.read()
    return ast.literal_eval(data)