#!/bin/python

#coding=utf-8


from math import log
from igraph import *
from random import randint
import pdb

sys.path.append("../data_handle")
from stream_data_tiles2 import Stream_Data_Tiles2
from stream_data_tiles3 import Stream_Data_Tiles3
from stream_data_tiles4 import Stream_Data_Tiles4
from stream_data_tiles5 import Stream_Data_Tiles5
from unionset import UnionSet

class Community_Detect_Our(object):
    def __init__(self, fname):
        self.g = None 
        self.stream_data = Stream_Data_Tiles5(fname)
        self.community_set_detected = {}
        #self.community_set_given = {}
        self.period = 10 * 30 * 60
        self.label_index = 0
        self.weight_matrix = {}
        self.common_nodes_matrix = {}
        self.similarity_matrix = {}
        self.similarity_sum = {}
        self.alpha = 1
        self.beta = 1

    def generate_new_label(self):
        self.label_index += 1
        return self.label_index
        
    def detect_community(self):
        for v in self.g.vs:
            v["label"] = self.generate_new_label()
            v["tmp_label"] = -1 
        self.cdcbs()
        #self.modified_lpa_asyn()
        #self.modified_lpa_syn()

    def get_common_nodes_matrix(self):
        self.common_nodes_matrix = {}
        for v1 in self.g.vs:
            node1 = v1["name"]
            self.common_nodes_matrix[node1] = {}
            for v2 in self.g.vs:
                node2 = v2["name"]
                if node1 == node2:
                    self.common_nodes_matrix[node1][node2] = 0
                    continue
                if node2 not in self.common_nodes_matrix[node1]:
                    self.common_nodes_matrix[node1][node2] = 0
                l1 = v1.neighbors()
                l2 = v2.neighbors()
                for item in l1:
                    if item in l2:
                        self.common_nodes_matrix[node1][node2] += 1

    def get_weight_matrix(self):
        self.weight_matrix = {}
        for v1 in self.g.vs:
            node1 = v1["name"]
            self.weight_matrix[node1] = {}
            for v2 in self.g.vs:
                node2 = v2["name"]
                edge_key = node1 + "_" + node2
                if edge_key in self.edge_dict:
                    self.weight_matrix[node1][node2] = self.edge_dict[edge_key][3]
                else:
                    self.weight_matrix[node1][node2] = 0

    def get_similarity_matrix(self):
        self.get_weight_matrix()
        self.get_common_nodes_matrix()
        self.similarity_matrix = {}
        for node1 in self.weight_matrix:
            self.similarity_matrix[node1] = {}
            for node2 in self.weight_matrix:
               self.similarity_matrix[node1][node2] = \
                       self.alpha * self.weight_matrix[node1][node2] +\
                       self.beta * self.common_nodes_matrix[node1][node2]

    def get_similarity_sum(self):
        self.get_similarity_matrix()
        self.similarity_sum = {}
        for node in self.similarity_matrix:
            self.similarity_sum[node] = 0
            for neighbor in self.similarity_matrix[node]:
                self.similarity_sum[node] += self.similarity_matrix[node][neighbor]

        return self.similarity_sum

    def get_average_similarity_sum(self):
        total = 0
        for node in self.similarity_sum:
            total += self.similarity_sum[node]

        return round(total * 1.0 / len(self.similarity_sum), 2)

    def generate_init_community(self, p):
        self.communities_detected = {}
        for node in p:
            v = self.g.vs.select(name = node)[0]
            self.communities_detected[v["label"]] = [node]

    def get_similarity_between_node_and_community(self, community, node_name):
        s = set(community)
        val = 0
        node = self.g.vs.select(name = node_name)[0]
        for neighbor in node.neighbors():
            neighbor_name = neighbor["name"]
            if neighbor_name in s:
                val += self.similarity_matrix[node_name][neighbor_name]  

        return val

    def add_node_to_communities(self, node):
        max_val = 0
        max_label = 0

        for label in self.communities_detected:
            val = self.get_similarity_between_node_and_community(self.communities_detected[label], \
                    node)
            if val > max_val:
                max_val = val
                max_label = label

        if max_label == 0:
            print "add node to communities error!"
            exit(0)

        self.communities_detected[max_label].append(node)

    def combinate_communities(self):
        ok = True
        node = None
        v_label = 0
        while ok:
            ok = False
            node = None
            for label in self.communities_detected:
                if len(self.communities_detected[label]) == 1:
                    node = self.g.vs.select(name = self.communities_detected[label][0])[0]
                    v_label = label
                    break
            if node != None:
                ok = True
                self.add_node_to_communities(node["name"])
                self.communities_detected.pop(v_label)

    def cdcbs(self):
        nodes_sim_sum = self.get_similarity_sum()
        similarity_average = self.get_average_similarity_sum()
        p = {}
        q = {}
        for node in nodes_sim_sum:
            if nodes_sim_sum[node] > similarity_average:
                p[node] = nodes_sim_sum[node]
            else:
                q[node] = nodes_sim_sum[node]
        self.generate_init_community(p)
        while len(q) > 0:
            k = sorted(q.items(), key=lambda item:item[1], reverse=True)
            q = {}
            for t in k:
                status = self.add_node_to_communities(t[0])
                if status == False:
                    q[t[0]] = t[1]

        self.combinate_communities()

    def modified_lpa_syn(self):
        l = []
        for v in self.g.vs:
            l.append(v)
        #l.sort(key=lambda v:v.degree(), reverse=True)
        ok = True
        i = 0
        count = 0
        while ok == True:
            ok = False
            for v in l:
                d = {}
                #d[v["label"]] = 1
                for n in v.neighbors():
                    try:
                        d[n["label"]] += 1
                    except:
                        d[n["label"]] = 1
                
                max_key = max(d, key=d.get)

                if max_key != v["label"]:
                    #v["label"] = max_key 
                    ok = True        

                v["tmp_label"] = max_key 

            for v in l:
                v["label"] = v["tmp_label"]

            i += 1
        
        #print "iteration_count: " + str(i)

    def modified_lpa_asyn(self):
        l = []
        for v in self.g.vs:
            l.append(v)
        l.sort(key=lambda v:v.degree(), reverse=True)
        ok = True
        i = 0
        count = 0
        while ok == True:
            ok = False
            for v in l:
                d = {}
                d[v["label"]] = 1
                for n in v.neighbors():
                    edge_key = v["name"] + "_" + n["name"]
                    if n["label"] not in d:
                        d[n["label"]] = 0
                    d[n["label"]] += self.edge_dict[edge_key][3]#g.es[eid]["weight"]
                
                max_key = max(d, key=d.get)
                '''
                max_value = d[max_key]
                tmp = []
                for key in d:
                    if d[key] == max_value:
                        tmp.append(key)
                ri = randint(0, len(tmp) - 1)
                max_key = tmp[ri]
                '''
                if max_key != v["label"]:
                    v["label"] = max_key 
                    ok = True        

                #v["tmp_label"] = max_key 

            #for v in l:
            #    v["label"] = v["tmp_label"]

            i += 1
        
        #print "iteration_count: " + str(i)

    def generate_graph(self, edge_list):
        self.g = Graph()
        self.node_dict = {}
        self.edge_dict = {}
        for edge in edge_list:
            if edge[0] not in self.node_dict:
                self.g.add_vertex(edge[0])
                self.node_dict[edge[0]] = -1

            if edge[1] not in self.node_dict:
                self.g.add_vertex(edge[1])
                self.node_dict[edge[1]] = -1

            edge_key1 = edge[0] + "_" + edge[1]
            edge_key2 = edge[1] + "_" + edge[0]

            if edge_key1 not in self.edge_dict and edge_key2 not in self.edge_dict:
                self.g.add_edge(edge[0], edge[1])
                self.edge_dict[edge_key1] = edge 
                self.edge_dict[edge_key2] = edge

        for edge in self.g.es:
            sid = edge.tuple[0]
            did = edge.tuple[1]
            sname = self.g.vs[sid]["name"]
            dname = self.g.vs[did]["name"]
            edge_key = sname + "_" + dname
            edge["weight"] = self.edge_dict[edge_key][3]
            
        print self.g.summary()
        
    def get_community_set(self):
        self.community_set_detected = {}
        for v in self.g.vs:
            if v["label"] not in self.community_set_detected:
                self.community_set_detected[v["label"]] = []
            self.community_set_detected[v["label"]].append(v["name"])
        return self.community_set_detected

    def get_confusion_matrix(self, csd, csg):
        n = []
        for i in range(0, len(csg)):
            n.append([])
        i = 0
        for label1 in csg:
            for label2 in csd:
                n[i].append(len(set(csg[label1]) & set(csd[label2])))
            i += 1

        return n

    def get_nmi_numerator(self, n):
        csdn = len(n[0])
        csgn = len(n)
        
        nn = 0
        for i in range(0, csgn):
            nn += sum(n[i])

        numerator = 0
        for i in range(0, csgn):
            ni = sum(n[i])
            for j in range(0, csdn):
                nj = 0
                for k in range(0, csgn):
                    nj += n[k][j]
                if nj == 0:
                    print "error!-------"
                    exit()
                if n[i][j] != 0:
                    numerator += n[i][j] * log(n[i][j] * nn * 1.0 / (ni * nj))

        return -2 * numerator

    def get_nmi_denominator(self, n):
        p1 = 0
        p2 = 0
        csdn = len(n[0])
        csgn = len(n)

        nn = 0
        for i in range(0, csgn):
            nn += sum(n[i])

        for i in range(0, csgn):
            ni = sum(n[i])    
            p1 += ni * log(ni * 1.0 / nn)
            
        for j in range(0, csdn):
            nj = 0
            for k in range(0, csgn):
                nj += n[k][j]
            if nj == 0:
                print "error"
            p2 += nj * log(nj * 1.0 / nn)

        return p1 + p2    
        
    def show_communities_info(self, communities):
        print len(communities)
        return
        i = 0 
        for key in communities:
            print communities[key]
            if i == 0:
                break
            print key, len(communities[key])

    def get_nmi(self):
        community_set_detected = self.get_community_set()
        #self.show_communities_info(community_set_detected)
        #print len(community_set_detected)
        #print community_set_detected.keys()
        community_set_given = self.stream_data.get_community_set()
        #self.show_communities_info(community_set_given)
        n = self.get_confusion_matrix(community_set_detected, community_set_given)
        numerator = self.get_nmi_numerator(n)
        denominator = self.get_nmi_denominator(n)

        return numerator * 1.0 / denominator

    def get_nmi_from_matrix(self, n):
        numerator = self.get_nmi_numerator(n)
        denominator = self.get_nmi_denominator(n)
        return numerator * 1.0 / denominator

    def get_subgraphs_1(self):
        subgraphs = {}
        ok = True
        i = 0
        while len(self.g.vs) > 0:
            a = set()
            b = set()
            a.add(self.g.vs[0])
            while a != b:
                for v in a:
                    b.add(v)
                    for n in v.neighbors():
                        b.add(n)
                for v in b:
                    a.add(v)
                    for n in v.neighbors():
                        a.add(n)
                #print len(a)
            subgraph = a
            #print ("%d, %d") % (i, len(subgraph))
            subgraphs[i] = subgraph
            i += 1
            id_list = [v.index for v in subgraph]
            self.g.delete_vertices(id_list)

        #print len(subgraphs)
        #return len(subgraphs)
        
        s = set([len(subgraphs[key]) for key in subgraphs])
        return s

    def get_subgraphs_2(self):
        u = UnionSet()
        for v in self.g.vs:
            u.init(v.index)
        for e in self.g.es:
            u.join(e.tuple[0], e.tuple[1])

        subgraphs = {}
        for v in self.g.vs:
            c = u.find(v.index)
            if c not in subgraphs:
                subgraphs[c] = []
            subgraphs[c].append(v.index)

        #return len(subgraphs)

        s = set([len(subgraphs[key]) for key in subgraphs])
        return s

    def probe_point(self):
        s2 = self.get_subgraphs_2()
        print "subgraphs count 2:", len(s2), s2#self.get_subgraphs_2()

        s1 = self.get_subgraphs_1()
        print "subgraphs count 1:", len(s1), s1#self.get_subgraphs_1()
            
    def test_case(self):    
        ok = True
        total_nmi = 0
        count = 0
        i = 0
        l = []
        a = 0  
        b = 20
        while ok and i < b:
            edge_list = self.stream_data.get_data_segment(self.period)
            i += 1
            if i < a:
                continue
            #print len(edge_list)
            if len(edge_list) < 10:
                #ok = False
                continue
            self.generate_graph(edge_list)
            #sub_count = self.probe_point()
            #if sub_count == 5:
            #    print edge_list[0]
            #continue
            self.detect_community()
            nmi = self.get_nmi()
            total_nmi += nmi
            count += 1
            print nmi
            #print "\n"
            l.append([i, nmi])
        print "average nmi: ", total_nmi / count
        #print sorted(l, key=lambda item : item[1])
    
def main():
    #fname = "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-17.csv"
    fname = "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-25.csv"
    #fname = "/home/zhoumin/project/community tracking/labeled_log0202/selected_2018-01-17.csv"
    #fname = "/home/zhoumin/project/community tracking/labeled_log0202/selected_2018-01-18.csv"
    #fname = "/home/zhoumin/project/community tracking/labeled_log0202/selected_2018-01-25.csv"
    #fname = "/home/zhoumin/project/community tracking/sselected/sselected_2018-01-25.csv"
    #fname = "/home/zhoumin/project/community tracking/sselected/sselected_2018-01-17.csv"
    #fname = "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-17.csv"

    cdo = Community_Detect_Our(fname)
    cdo.test_case()
    '''
    n = [[2, 1, 0], [0, 1, 1]]
    n = [[3,2]]
    n = [[1]]
    print cdo.get_nmi_from_matrix(n)
    '''


if __name__ == "__main__":
    main()
