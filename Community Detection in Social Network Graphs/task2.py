from pyspark import SparkContext, SparkConf
import sys
import time 
import queue, operator, copy



class Graph:

    def __init__(self, ids, parents, children, shortest_path, weight, level):
        self.ids = ids
        self.parents = parents
        self.children = children
        self.shortest_path = shortest_path
        self.weight = weight
        self.level = level

class BFSTree:

    def __int__(self, root, tree):
        self.root = root
        self.tree = tree


def make_graph(x):

    neighbors_set = set()
    same_review_num = 0
    for key,val in user_business_map.items():
        if(key != x[0]):
            same_review_num = len(val.intersection(x[1]))
            if same_review_num >= threshold:
                neighbors_set.add(key)

    return (x[0], neighbors_set)

def bfs(root, graph):
    nodes_visited = dict()
    root_node = Graph(root, set(), set(), 0, 0, 0)
    nodes_visited[root] = root_node
    level = 1
    Q = queue.Queue()
    Q.put(root)
    while not Q.empty():
        qsize = Q.qsize()
        for x in range(qsize):
            current_node = Q.get()
            for p in graph[current_node]:
                if(p not in nodes_visited):
                    node = Graph(p, set(), set(), 0, 0, level)
                    node.parents.add(nodes_visited[current_node])
                    nodes_visited[current_node].children.add(node)
                    Q.put(p)
                    nodes_visited[p] = node
                else:
                    if(nodes_visited[current_node].level != nodes_visited[p].level and nodes_visited[p] not in nodes_visited[current_node].parents):
                        nodes_visited[current_node].children.add(nodes_visited[p])
                        nodes_visited[p].parents.add(nodes_visited[current_node])
        level = level+ 1

    return nodes_visited


def choose_shortest_path(tree, root):
    tree[root].shortest_path = 1
    Q = queue.Queue()
    Q.put(tree[root])
    nodes_visited = set()
    nodes_visited.add(root)
    level1 = dict()
    while not Q.empty():
        current_node = Q.get()
        if(current_node.level not in level1):
            level1[current_node.level] = set()
            level1[current_node.level].add(current_node)
        else:
            level1[current_node.level].add(current_node)

        if(len(current_node.parents) == 0):
            current_node.shortest_path = 1
        else:
            sp = 0
            for p in current_node.parents:
                sp = sp + (p.shortest_path)
            current_node.shortest_path = sp

        for ch in current_node.children:
            if(ch.ids not in nodes_visited):
                nodes_visited.add(ch.ids)
                Q.put(ch)

    return level1


def fetch_edges(c, p):
    edge = [c.ids, p.ids]
    edge.sort()
    return tuple(edge)


def assign_outgoing_edge(node, edge_map):
    if(len(node.parents) > 0):
        total_sp = 0.0
        for p in node.parents:
            total_sp = total_sp +  (p.shortest_path)
        for p in node.parents:
            edge = fetch_edges(node, p)
            edge_map[edge] = ( p.shortest_path / float(total_sp) ) * node.weight

def credit_allocation(level_tree, edge_map):
    for pair in level_tree:
        nodes = pair[1]
        for node in nodes:
            if(len(node.children) == 0):
                node.weight = 1.0
                assign_outgoing_edge(node, edge_map)

            else:
                edge_sum = 0.0
                for ch in node.children:
                    edge = fetch_edges(node, ch)
                    edge_sum = edge_sum +  edge_map[edge]
                node.weight = 1 + edge_sum

                assign_outgoing_edge(node, edge_map)

def remove_edges(g, edge_rem):
    g[edge_rem[0][0]].discard(edge_rem[0][1])
    g[edge_rem[0][1]].discard(edge_rem[0][0])


def calculate_q(graph, m, a, sss):
    Q = list()
    for ss in sss:
        s = 0.0
        for i in ss:
            for j in ss:
                A = 0
                if(j in a[i]):
                    A = 1
                else:
                    A = 0
                expected_edges = (len(a[i]) * len(a[j]) / (2.0 * m))
                s = s +  (A - expected_edges) / (2.0 * m)
        Q.append(s)
    return sum(Q)


def calculate_betweenness(graph):
    tree_map = dict()
    for k, v in graph.items():
        curr_tree = bfs(k, graph)
        tree_map[k] = curr_tree


    level_map = dict()
    for k, v in tree_map.items():
        gn_tree = tree_map[k]
        level_map[k] = choose_shortest_path(gn_tree, k)

  
    credit_map = dict()
    for key1, val in level_map.items():
        edge_map = dict()
        level_ordered = sorted(level_map[key1].items(), key = operator.itemgetter(0), reverse = True)
        credit_allocation(level_ordered, edge_map)
        credit_map[key1] = edge_map

    betweenness_map = dict()
    for key, val in credit_map.items():
        for e, w in val.items():
            if e not in betweenness_map:
                betweenness_map[e] = list()
                betweenness_map[e].append(w)

            else:
                betweenness_map[e].append(w)

    for key, val in betweenness_map.items():
        betweenness_map[key] = (sum(val)) / 2.0

    btn_sorted = [(key, val) for key , val in betweenness_map.items()]
    btn_sorted.sort()
    btn_sorted.sort(key = lambda x : x[1], reverse=True)
    return btn_sorted


def community_detection(node, graph, current_node):
    nodes_visited = set()
    nodes_visited.add(node)
    Q = queue.Queue()

    Q.put(node)
    while not Q.empty():
        qsize = Q.qsize()
        for x in range(qsize):
            current = Q.get()
            for nei in graph[current]:
                if nei not in nodes_visited:
                    nodes_visited.add(nei)
                    Q.put(nei)
                    current_node.add(nei)

    return nodes_visited


def find_communities(g1):
    current_graph = copy.deepcopy(g1)
    current_node = set()
    communities = list()
    for key, val in current_graph.items():
        if(key not in current_node):
            current_node.add(key)
            comm = community_detection(key, current_graph, current_node)
            communities.append(list(comm))
    return communities


def community_detection_final(time_iteration, m, edge_highest_btn, graph, A, qs):
    comms = None
    m_num_edges = m
    for r in range(time_iteration):
        q = 0
        if r == 0:
            remove_edges(graph, edge_highest_btn)
            comms = find_communities(graph)
            q = calculate_q(graph, m_num_edges, A, comms)
            qs.append(q)
            edge_highest_btn = calculate_betweenness(graph)[0]
        else:
            remove_edges(graph, edge_highest_btn)
            comms = find_communities(graph)
            q = calculate_q(graph, m_num_edges, A, comms)
            qs.append(q)

            if r != m_num_edges - 1:
                edge_highest_btn = calculate_betweenness(graph)[0]

    return comms



start = time.time()
sc = SparkContext().getOrCreate()
threshold= int(sys.argv[1])
data = sc.textFile(sys.argv[2])
header =data.first()
data = data.filter(lambda x : x != header)
user_business = data.map(lambda x : x.split(",")).map(lambda x : (x[0], [x[1]])).reduceByKey(lambda a, b : a + b)
user_business = user_business.map(lambda x : (x[0], set(x[1])))
user_business_map = user_business.collectAsMap()
s1 = time.time()
graph = user_business.map(make_graph).filter(lambda x : len(x[1]) != 0).collectAsMap()
print('\ntime: ' + str(time.time() - s1))
btn_sorted = calculate_betweenness(graph)
ind = btn_sorted[-1]
with open(sys.argv[3],'w') as fw:
    for i in btn_sorted:
        fw.write("('"+str(i[0][0])+"', '"+str(i[0][1])+"'), "+str(i[1]))
        if(i!=ind):
            fw.write("\n")

m_num_edges = len(btn_sorted)
A = copy.deepcopy(graph)
qs = list()
edge_highest_btn = btn_sorted[0]
community_detection_final(m_num_edges, m_num_edges, edge_highest_btn, graph, A, qs)
q_index = qs.index(max(qs))
qs = list()
graph = copy.deepcopy(A)
output_comms = community_detection_final(q_index + 1, m_num_edges, btn_sorted[0], graph, A, qs)

for com in output_comms:
    com.sort()


output_comms.sort()
output_comms.sort(key = lambda x : len(x))
    
last=output_comms[-1]
with open(sys.argv[4],'w') as fw:
    for i in output_comms:
        s=""
        if(len(output_comms)== 1):
            for j in i:
                s=s+"'"+j+"'"
             
        else:
            for j in i:
                s=s+"'"+j+"', "
        
        s=s.rstrip(', ')
        fw.write(s)
        if(i!=last):
            fw.write("\n")
    
print('\ntime: ' + str(time.time() - s1))
