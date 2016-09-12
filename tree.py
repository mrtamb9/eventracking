CHILDRENT = 'childrent'
ID  = 'id'
import json,util
def get_node():
    node = dict()
    node[ID] = -1
    node[CHILDRENT] = []
    return node
def get_node_with_id(node_id):
    node = dict()
    node[ID] = node_id
    node[CHILDRENT] = []
    return  node
def get_node_with_id_childrent(node_id, childrent):
    node = dict()
    node[ID] = node_id
    node[CHILDRENT]=childrent
    return node
def add_node(node,child):
    node[CHILDRENT].append(child)

def is_leaf(node):
    if (len(node[CHILDRENT]) == 0):
        return True
    return False
def get_leaves(node):
    if is_leaf(node):
        return [node]
    result = []
    for child in node[CHILDRENT]:
        leaves = get_leaves(child)
        for leaf in leaves:
            result.append(leaf)
    return result

def get_str(node):
    return json.dumps(node)

def get_node_from_str(obstr):
    return util.str_to_dict(obstr)
def get_nodes_from_file(filepath):
    f = open(filepath,'r')
    text = f.readline()
    f.close()
    nodes = json.loads(text)
    return nodes