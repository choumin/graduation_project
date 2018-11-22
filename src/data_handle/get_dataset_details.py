# -*- coding: utf-8 -*-
from igraph import *
import multiprocessing
from stream_data_tiles3 import Stream_Data_Tiles3

def add_stream_data(data):
    #item[0] --- source
    #item[1] --- target
    #item[2] --- label
    #item[3] --- time
    g = Graph()
    node_dict = {}
    edge_dict = {}
    for item in data:
        if len(item) == 0:
            continue

        if item[0] not in node_dict:
            g.add_vertex(item[0])
            node_dict[item[0]] = -1

        if item[1] not in node_dict:
            g.add_vertex(item[1])
            node_dict[item[1]] = -1

        edge_key1 = item[0] + "-" + item[1]
        edge_key2 = item[1] + "-" + item[0]

        if edge_key1 not in edge_dict and edge_key2 not in edge_dict:
            g.add_edge(item[0], item[1])

        edge_dict[edge_key1] = item[2]
        edge_dict[edge_key2] = item[2]

    print "add stream data done!"
    print g.summary()

    return g, node_dict, edge_dict


def get_details2(args):
    d = args[1]
    fname = args[0]
    print fname
    #return 0
    sd = Stream_Data_Tiles3(fname)
    interval = 24 * 60 * 60
    data = sd.get_data_segment(interval)
    print "data len: ", len(data)
    g, node_dict, edge_dict = add_stream_data(data)
    return 0
    maxv = 0
    minv = 10000
    for v in g.vs:
        num = len(v.neighbors())
        if num > maxv:
            maxv = num
        if num < minv:
            minv = num
        d["nodes"].add(v["name"])

    for e in g.es:
        source_id = e.tuple[0] 
        target_id = e.tuple[1] 
        source_name = g.vs[source_id]["name"]
        target_name = g.vs[target_id]["name"]
        edge_key = source_name + "-" + target_name
        d["edges"].add(edge_key)

        
    d["max"] = maxv
    d["min"] = minv
    
    

def main2():
    files = ["/home/zhoumin/project/community tracking/labeled_log0202/2018-01-17.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-18.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-19.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-20.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-21.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-22.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-23.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-24.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-25.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-26.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-27.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-28.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-29.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-30.csv"]
    total = {}
    days = []
    for i in range(0, len(files)):
        days.append({})
        days[i]["max"] = 0
        days[i]["min"] = 10000
        days[i]["nodes"] = set([])
        days[i]["edges"] = set([])

    total["max"] = 0
    total["min"] = 10000
    total["nodes"] = set([])
    total["edges"] = set([])

    p = multiprocessing.Pool(14)
    p.map(get_details2, [[files[i], days[i]] for i in range(0, len(files))])
    
    for day in days:
        if day["max"] > total["max"]:
            total["max"] = day["max"]
        if day["min"] < total["min"]:
            total["min"] = day["min"]
        for node in day["nodes"]:
            total["nodes"].append(node)
        for edge in day["edges"]:
            total["edges"].append(edge)

    print total["max"], total["min"], len(total["nodes"]), len(total["edges"])

def get_src_dst(line):
    line = line.strip()
    begin = 0
    end = line.find(',', begin)
    src = line[begin : end]

    begin = end + 1
    end = line.find(',', begin)
    dst = line[begin : end]

    return src, dst
    #items = line.strip().split(",")
    #return items[0], items[1]

def get_date(fname):
    begin = fname.rfind('/')
    end = fname.rfind('.')

    return fname[begin + 1 : end]

def get_details(args):
    fname = args[0]
    day = args[1]

    day["date"] = get_date(fname)
    print fname
    i = 0
    fp = file(fname)
    line = fp.readline()
    while line:
        src, dst = get_src_dst(line)

        if src not in day["nodes"]:
            day["nodes"][src] = set([])
        day["nodes"][src].add(dst)

        if dst not in day["nodes"]:
            day["nodes"][dst] = set([])
        day["nodes"][dst].add(src)

        edge1 = src + "-" + dst
        edge2 = dst + "-" + src
        day["edges"].add(edge1)
        day["edges"].add(edge2)

        day["flows"] += 1

        line = fp.readline()
        i += 1

    print fname, i

    fp.close()

    return day

def main():
    files = ["/home/zhoumin/project/community tracking/labeled_log0202/2018-01-17.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-18.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-19.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-20.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-21.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-22.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-23.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-24.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-25.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-26.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-27.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-28.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-29.csv", \
            "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-30.csv"]

    nodes = {}
    edges = set([])
    days = []
    flows = 0
    for i in range(0, len(files)):
        days.append({})
        days[i]["date"] = ""
        days[i]["nodes"] = {}
        days[i]["edges"] = set([])
        days[i]["flows"] = 0

    p = multiprocessing.Pool(14)
    results = p.map(get_details, [[files[i], days[i]] for i in range(0, len(files))])

    for day in results:
        print day["date"], len(day["nodes"]), len(day["edges"]) / 2, day["flows"]
        for node in day["nodes"]:
            if node not in nodes:
                nodes[node] = set([])
            nodes[node] = nodes[node].union(day["nodes"][node])
        edges = edges.union(day["edges"])
        flows += day["flows"]

    mind = 100000
    maxd = 0
    freq = {}
    for node in nodes:
        deg = len(nodes[node])
        if deg > maxd:
            maxd = deg
        if deg < mind:
            mind = deg
        if deg not in freq:
            freq[deg] = 0
        freq[deg] += 1

    items = sorted(freq.keys())
    begin = 20
    count = 0
    for item in items:
        if item <= begin:
            print item, round(freq[item] * 1.0 / len(nodes), 4)
            continue
        count += 1
        if count % 10 == 0:
            print item, round(freq[item] * 1.0 / len(nodes), 4)
            
    print len(nodes), len(edges) / 2, flows, maxd, mind, len(edges) * 1.0 / len(nodes)

if __name__ == "__main__":
    main()
