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

class Community_Detect_CommTracker(object):
    def __init__(self, fname):
        self.g = None 
        self.stream_data = Stream_Data_Tiles5(fname)
        self.community_set_detected = {}
        #self.community_set_given = {}
        self.period = 30 * 60
        self.label_index = 0

    def generate_new_label(self):
        self.label_index += 1
        return self.label_index
        
    def detect_community(self):
        for v in self.g.vs:
            v["label"] = self.generate_new_label()
            v["tmp_label"] = -1 
        
        #self.original_infomap()
        self.original_blondel()

    def original_blondel(self):
        self.membership_vector = self.g.community_multilevel()
        
    def original_infomap(self):
        self.membership_vector = self.g.community_infomap()

    def original_lpa(self):
        self.membership_vector = self.g.community_label_propagation()
        '''
        self.correct_ratio = self.get_correct_ratio()
        for item in self.correct_ratio:
            print item[0], item[1], item[2]

        #print self.membership_vector
        self.get_community_radius()
        self.modularity_list.append(self.g.modularity(self.membership_vector))
        '''
        
    def modified_lpa_syn(self):
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
                #d[v["label"]] = 1
                for n in v.neighbors():
                    try:
                        d[n["label"]] += 1
                    except:
                        d[n["label"]] = 1
                
                max_key = max(d, key=d.get)

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
                self.edge_dict[edge_key1] = -1
                self.edge_dict[edge_key2] = -1
            
        #print self.g.summary()
        
    def get_community_set(self):
        self.community_set_detected = {}
        for community in self.membership_vector:
            self.community_set_detected[community[0]] = []
            for idx in community:
                self.community_set_detected[community[0]].append(self.g.vs[idx]["name"])

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
        #print community_set_detected.keys()
        community_set_given = self.stream_data.get_community_set()
        #self.show_communities_info(community_set_given)
        n = self.get_confusion_matrix(community_set_detected, community_set_given)
        numerator = self.get_nmi_numerator(n)
        denominator = self.get_nmi_denominator(n)

        return numerator * 1.0 / denominator

    def test_case(self):    
        ok = True
        total_nmi = 0
        count = 0
        a = 0
        b = 20
        i = 0
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
            self.detect_community()
            nmi = self.get_nmi()
            total_nmi += nmi
            count += 1
            print nmi
            #print "\n"
        print "average nmi: ", total_nmi / count
    
def main():
    #fname = "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-17.csv"
    fname = "/home/zhoumin/project/community tracking/labeled_log0202/selected_2018-01-17.csv"
    #fname = "/home/zhoumin/project/community tracking/sselected/sselected_2018-01-25.csv"
    #fname = "/home/zhoumin/project/community tracking/sselected/sselected_2018-01-17.csv"
    cdc = Community_Detect_CommTracker(fname)
    cdc.test_case()


if __name__ == "__main__":
    main()
