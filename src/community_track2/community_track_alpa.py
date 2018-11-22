#!/bin/python

#coding=utf-8

from igraph import *
from random import randint
#import datetime
import pdb
import time

sys.path.append("../data_handle")
from stream_data_tiles3 import Stream_Data_Tiles3

class Vertex(object):
    def __init__(self, vertex):
        self.last_label = vertex["last_label"]
        self.label = vertex["label"]
        self.community_set = {}
    
class Community_Evolution(object):
    def __init__(self, label, time_point, community_events):
        self.label = label
        self.time_point = time_point
        self.community_events = community_events
    
    def get_events_details(self):
        s = str(self.label) + ", " + str(self.time_point) + ": " + str(self.community_events["size"])
        if self.community_events["appear"] == True:
            s += ", appear"
        if self.community_events["disappear"] == True:
            s += ", disappear"
        if self.community_events["bigger"] == True:
            s += ", bigger"
        if self.community_events["smaller"] == True:
            s += ", samller"
        if self.community_events["keep"] == True:
            s += ", keep"
        if self.community_events["split"]: 
            s += ", split(" 
            for i in range(0, len(self.community_events["split"])):
                if i == 0:
                    s += str(self.community_events["split"][i])
                else:
                    s += ", " + str(self.community_events["split"][i])
            s += ")"
        if self.community_events["merge"]: 
            s += ", merge(" 
            for i in range(0, len(self.community_events["merge"])):
                if i == 0:
                    s += str(self.community_events["merge"][i])
                else:
                    s += ", " + str(self.community_events["merge"][i])
            s += ")"

    
        return s 

class Community_Tracking(object):
    def __init__(self, fout_name):
        self.community_trace = {}
        self.event_statistics = {}
        self.fp = open(fout_name, 'w')
        
    def __del__(self):
        self.fp.close()
    
    def add_events(self, label, time_point, community_events):
        if label not in self.community_trace:
            self.community_trace[label] = []
        self.community_trace[label].append(Community_Evolution(label, time_point, community_events))    
            
    def get_average_longevity(self):
        tmp = [len(self.community_trace[label]) for label in self.community_trace]
        return sum(tmp) * 1.0 / len(tmp)

    def get_max_longevity(self):
        tmp = [len(self.community_trace[label]) for label in self.community_trace]
        return max(tmp)

    def get_min_longevity(self):
        tmp = [len(self.community_trace[label]) for label in self.community_trace]
        return min(tmp)
        
    def show_one_community_evolution(self, label):
        if label not in self.community_trace:
            s = "wrong label: " + str(label)
            print s, "\n"
            return
        for ce in self.community_trace[label]:
            print ce.get_events_details()  
        print "\n"

    def show_event_occur_probability(self):
        events = {}
        first_time = True
        for label in self.community_trace:
            community_evolution_list = self.community_trace[label]
            for community_evolution in community_evolution_list:
                time_point = community_evolution.time_point
                if time_point not in events:
                    events[time_point] = {}
                    if first_time:
                        events[time_point]["appear"] = 0
                        events[time_point]["disappear"] = 0
                        events[time_point]["smaller"] = 0
                        events[time_point]["bigger"] = 0
                        events[time_point]["keep"] = 0
                        first_time = False
                    else:
                        events[time_point]["appear"] = 1
                        events[time_point]["disappear"] = 1
                        events[time_point]["smaller"] = 1
                        events[time_point]["bigger"] = 1
                        events[time_point]["keep"] = 1
                    events[time_point]["merge"] = 0
                    events[time_point]["split"] = 0
                if community_evolution.community_events["appear"]:
                    events[time_point]["appear"] += 1 
                if community_evolution.community_events["disappear"]:
                    events[time_point]["disappear"] += 1
                if community_evolution.community_events["merge"]:
                    events[time_point]["merge"] += 1
                if community_evolution.community_events["split"]:
                    events[time_point]["split"] += 1
                if community_evolution.community_events["smaller"]:
                    events[time_point]["smaller"] += 1
                if community_evolution.community_events["bigger"]:
                    events[time_point]["bigger"] += 1
                if community_evolution.community_events["keep"]:
                    events[time_point]["keep"] += 1

        for time_point in events:
            tmp_sum = sum(events[time_point].values())
            tmp_sum *= 1.0
            events[time_point]["appear"] /= tmp_sum 
            events[time_point]["disappear"] /= tmp_sum 
            events[time_point]["merge"] /= tmp_sum 
            events[time_point]["split"] /= tmp_sum 
            events[time_point]["bigger"] /= tmp_sum 
            events[time_point]["smaller"] /= tmp_sum 
            events[time_point]["keep"] /= tmp_sum 

        for time_point in events:
            s = "%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f" % \
                    (time_point, \
                    events[time_point]["appear"], \
                    events[time_point]["disappear"], \
                    events[time_point]["keep"], \
                    events[time_point]["bigger"], \
                    events[time_point]["smaller"], \
                    events[time_point]["split"], \
                    events[time_point]["merge"])
            self.fp.write(s + "\n")
            print s

    def show_all_community_evolution(self):
        l = sorted(self.community_trace.items(), key=lambda item: len(item[1]), reverse=True) 
        
        for item in l:
            self.show_one_community_evolution(item[0])
        #for label in self.community_trace:
        #    self.show_one_community_evolution(label)
        
class Network_Measure(object):
    def __init__(self, window, increment):
        self.g = Graph()
        self.g.vs["label"] = []
        self.g.vs["name"] = []
        self.label_dict = {}
        self.node_dict = {}
        self.edge_dict = {}
        self.new_node = {}
        self.split_threshold = 0.2
        self.merged_threshold = 0.2
        self.keep_threshold = 0.5
        self.current_time = 0
        self.interval = window 
        self.increment = increment 
        self.label_index = 0
        self.current_community_set = {}
        self.last_community_set = {}
        self.last_vertex_dict = {}
        self.time_point = 0 
        self.coefficient = 1

    def get_label_dict_from_stream(self, stream):
        self.label_dict = {}
        for item in stream:
            #print item[0]
            if len(item) == 0:
                continue
            if item[0] not in self.label_dict:
            #if not self.label_dict[item[0]]:
            #if type(self.label_dict[item[0]]) != dict:
                self.label_dict[item[0]] = {}

            if item[1] not in self.label_dict:
            #if not self.label_dict[item[1]]:
            #if type(self.label_dict[item[1]]) != dict:
                self.label_dict[item[1]] = {}
            
            try:
                self.label_dict[item[0]][item[2]] += 1
            except:
                self.label_dict[item[0]][item[2]] = 1 

            try:
                self.label_dict[item[1]][item[2]] += 1
            except:
                self.label_dict[item[1]][item[2]] = 1 
               
        for node in self.label_dict:
            key = max(self.label_dict[node], key=self.label_dict.get)
            self.label_dict[node]["main_label"] = key
            #print node, key

            #self.label_dict[item[1]] = item[2]

    def get_edge_set_from_stream(self, stream):
        return stream

        #data = []
        #for item in stream:
        #    data.append(item[0 : 2])

        #return data

    def get_name_index_dict(self):
        d = {}
        indexlist = [v.index for v in self.g.vs]
        namelist = [v["name"] for v in self.g.vs]
        d = dict(zip(namelist, indexlist))

        return d
    
    def add_stream_data(self, data):
        #self.get_label_dict_from_stream(data)
        #edge_set = self.get_edge_set_from_stream(data)
        self.new_node = {}
        
        #item[0] --- source
        #item[1] --- target
        #item[2] --- label
        #item[3] --- time
        for item in data:
            if len(item) == 0:
                continue

            if item[0] not in self.node_dict:
                self.g.add_vertex(item[0])
                self.node_dict[item[0]] = -1
                self.new_node[item[0]] = -1

            if item[1] not in self.node_dict:
                self.g.add_vertex(item[1])
                self.node_dict[item[1]] = -1
                self.new_node[item[1]] = -1

            edge_key1 = item[0] + "-" + item[1]
            edge_key2 = item[1] + "-" + item[0]

            if edge_key1 not in self.edge_dict and edge_key2 not in self.edge_dict:
                self.g.add_edge(item[0], item[1])

            self.edge_dict[edge_key1] = item[2]
            self.edge_dict[edge_key2] = item[2]

        for e in self.g.es:
            source_id = e.tuple[0] 
            target_id = e.tuple[1] 
            source_name = self.g.vs[source_id]["name"]
            target_name = self.g.vs[target_id]["name"]
            edge_key1 = source_name + "-" + target_name
            #edge_key2 = target_name + "-" + source_name
            e["time"] = self.edge_dict[edge_key1]

        print "add stream data done!"
        print self.g.summary()

    def set_label_to_node(self):
        for v in self.g.vs:
            v["label"] = self.label_dict[v["name"]]["main_label"] 

    def modularity(self, sd):
        i = 0
        test_count = 100
        while i < test_count:
            mytime, data = sd.get_data_segment(sd.interval)
            self.g = Graph()
            self.add_stream_data(data)
            self.set_label_to_node()
            self.membership_vector = clustering.VertexClustering.FromAttribute(self.g, "label") 
            print "modularity: ", self.g.modularity(self.membership_vector)
            i += 1

    def change_rate(self, sd):
        i = 0
        test_count = 1000
        total_count = 0
        while i < test_count:
            mytime, data = sd.get_data_segment(sd.interval)
            dateArray = datetime.datetime.utcfromtimestamp(mytime / 1000)
            otherStyleTime = dateArray.strftime("%Y-%m-%d_%H")
            print otherStyleTime + ":00:00\t" + str(len(data))
            total_count += len(data)
            i += 1
            if len(data) == 0:
                break
            
        print total_count * 1.0 / i 
    
    def init_graph(self):
        pass
         
    def get_init_graph(self, sd):
        self.current_time = sd.get_init_time()
        self.time_point += 1 
        print "init time: ", self.current_time
        #pdb.set_trace()
        data = sd.get_data_segment(self.interval)
        self.add_stream_data(data)
        #self.init_graph(data)
    
    def update_graph(self, sd):
        data = sd.get_data_segment(self.increment)
        self.time_point += 1 
        self.current_time += self.increment
        self.new_node = {}
        self.changed_node = {}
        #update the time of edge
        for item in data:
            if len(item) == 0:
                continue

            if item[0] not in self.node_dict:
                self.g.add_vertex(item[0])
                self.node_dict[item[0]] = -1
                self.new_node[item[0]] = -1

            if item[1] not in self.node_dict:
                self.g.add_vertex(item[1])
                self.node_dict[item[1]] = -1
                self.new_node[item[1]] = -1

            edge_key1 = item[0] + "-" + item[1]
            edge_key2 = item[1] + "-" + item[0]
            if edge_key1 not in self.edge_dict and edge_key2 not in self.edge_dict:
                self.g.add_edge(item[0], item[1])
                self.changed_node[item[0]] = -1
                self.changed_node[item[1]] = -1


            self.edge_dict[edge_key1] = item[2]
            self.edge_dict[edge_key2] = item[2]
            
        for e in self.g.es:
            source_id = e.tuple[0]
            target_id = e.tuple[1]
            source_name = self.g.vs[source_id]["name"]
            target_name = self.g.vs[target_id]["name"]
            edge_key1 = source_name + "-" + target_name
            edge_key2 = target_name + "-" + source_name
            
            if edge_key1 in self.edge_dict:
                e["time"] = self.edge_dict[edge_key1]
            #elif edge_key2 in self.edge_dict:
            #    e["time"] = self.edge_dict[edge_key2]
            
            if e["time"] < self.current_time:
                self.g.delete_edges(e.index)
                self.changed_node[source_name] = -1
                self.changed_node[target_name] = -1

                #self.edge_dict.__delitem__(edge_key1)
                #self.edge_dict.__delitem__(edge_key2)
                del self.edge_dict[edge_key1]
                del self.edge_dict[edge_key2]
                #The id of node will change after deleting a node.
                if self.g.vs[source_id].degree() == 0:
                    if self.g.vs[target_id].degree() == 0:
                        self.g.delete_vertices([source_id, target_id])
                        del self.node_dict[source_name]
                        del self.node_dict[target_name]
                    else:
                        self.g.delete_vertices(source_id)
                        del self.node_dict[source_name]
                elif self.g.vs[target_id].degree() == 0:
                    self.g.delete_vertices(target_id)
                    del self.node_dict[target_name]

        print "update stream data done!"
        print self.g.summary()

        return 
        
    def detect_init_community(self, ct):
        for v in self.g.vs:
            if v["name"] in self.new_node:
                v["label"] = self.generate_new_label()
                v["last_label"] = -1
            else:
                v["last_label"] = v["label"]
        
        self.active_list = {}
        for v in self.g.vs:
            self.active_list[v] = "active"
    
        self.llp()
        self.analyze_evolution(ct)
    
    def generate_new_label(self):
        self.label_index += 1
        return self.label_index

    def get_membership_vector(self):
        self.membership_vector = clustering.VertexClustering.FromAttribute(self.g, "label") 
        self.modularity_list.append(self.g.modularity(self.membership_vector))

    def get_neighbor_score(self, v, n):
        a = v.neighbors()
        b = n.neighbors()
        s = 0
        count = 0
        for item in b:
            if item in a:
                count += 1
        s = 1 + self.coefficient * count

        return s

    def update_label(self, v):
        d = {}
        s = {}
        for n in v.neighbors():
            if n["label"] not in d:
                d[n["label"]] = []
            d[n["label"]].append(n)
        for label in d:
            score = 0
            for n in d[label]:
                score += self.get_neighbor_score(v, n)
            s[label] = score
        l = []
        max_label = max(s, key=s.get)
        l.append(max_label)
        for label in s:
            if s[label] == s[max_label]:
                l.append(label)
        ri = randint(0, len(l) - 1)
        is_active = False 
        if l[ri] != v["label"]:
            is_active = True 
            v["label"] = l[ri]
        
        return is_active

    def llp(self):
        i = 0
        while len(self.active_list) > 0:
            ri = randint(0, len(self.active_list) - 1)
            v = self.active_list.keys()[ri]
            is_active = self.update_label(v)
            if is_active:
                for n in v.neighbors():
                    self.active_list[n] = "active"
            else:
                self.active_list.pop(v)

            i += 1

        print "iteration count: ", i

    def modified_lpa(self):
        self.current_community_set = {}

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
                for n in v.neighbors():
                    try:
                        d[n["label"]] += 1
                    except:
                        d[n["label"]] = 1
                
                max_key = max(d, key=d.get)

                if max_key != v["label"]:
                    v["label"] = max_key 
                    ok = True        
            i += 1
        
        print "iteration_count: " + str(i)

    def get_community_details(self):
        community_set = {}
        vertex_dict = {}
        for v in self.g.vs:
            if v["label"] not in community_set:
                community_set[v["label"]] = [] 
            community_set[v["label"]].append(v['name'])
            vertex_dict[v['name']] = Vertex(v)
        
        return community_set, vertex_dict
    
    def analyze_evolution(self, ct):
        community_events = {} 
        community_set, vertex_dict = self.get_community_details()
        #change to six events
        for label in self.last_community_set:
            if label not in community_events:
                community_events[label] = {}
                community_events[label]["size"] = self.get_community_size(community_set, vertex_dict, label)
                community_events[label]["appear"] = False 
                community_events[label]["disappear"] = False 
                community_events[label]["bigger"] = False 
                community_events[label]["smaller"] = False 
                community_events[label]["keep"] = False 
                community_events[label]["merge"] = [] 
                community_events[label]["split"] = []
        for label in community_set:
            if label not in community_events:
                community_events[label] = {}
                community_events[label]["size"] = self.get_community_size(community_set, vertex_dict, label)
                community_events[label]["appear"] = False 
                community_events[label]["disappear"] = False 
                community_events[label]["bigger"] = False 
                community_events[label]["smaller"] = False 
                community_events[label]["keep"] = False 
                community_events[label]["merge"] = [] 
                community_events[label]["split"] = []
        
        for label in community_events:
            if self.is_appeared(community_set, vertex_dict, label):
                community_events[label]["appear"] = True    
            else:
                if self.is_disappeared(community_set, vertex_dict, label):
                    community_events[label]["disappear"] = True 
                else:     
                    if self.is_bigger(community_set, vertex_dict, label):
                        community_events[label]["bigger"] = True 
                    elif self.is_smaller(community_set, vertex_dict, label):
                        community_events[label]["smaller"] = True 
                    else:
                        community_events[label]["keep"] = True 

                    if self.is_merged(community_set, vertex_dict, label):
                        community_events[label]["merge"] = self.merged_details

                if self.is_split(community_set, vertex_dict, label):
                    community_events[label]["split"] = self.split_details
            #community_events[label]["size"] = self.get_community_size(community_set, vertex_dict, label)
                
        for label in community_events:
            ct.add_events(label, self.time_point, community_events[label]) 
            #print str(label) + ", " + community_events[label]["existence"] + ", " + community_events[label]["size"] + ", " + community_events[label]["structure"] 

        self.reserve_community_set(community_set, vertex_dict)

    def get_community_size(self, community_set, vertex_dict, label):
        size = 0
        if label in community_set:
            size = len(community_set[label])

        return size
    
    def is_disappeared(self, community_set, vertex_dict, label):
        if label in community_set:
            return False
        else:
            return True

    def is_appeared(self, community_set, vertex_dict, label):
        if label not in self.last_community_set and label in community_set:
            return True
        else:
            return False

    def is_bigger(self, community_set, vertex_dict, label):
        if len(self.last_community_set[label]) < len(community_set[label]): 
            return True
        else:
            return False

    def is_smaller(self, community_set, vertex_dict, label):
        if len(self.last_community_set[label]) > len(community_set[label]): 
            return True
        else:
            return False

    def is_split(self, community_set, vertex_dict, label):
        d = {}
        for vname in self.last_community_set[label]:
            if vname not in vertex_dict:
                continue
            tmp_label = vertex_dict[vname].label
            if tmp_label not in d:
                d[tmp_label] = 0
            d[tmp_label] += 1
        community_size = len(self.last_community_set[label])
        l = sorted(d.items(), key=lambda item : item[1], reverse=True) 
        ok = False
        self.split_details = [] 
        for item in l:
            #The size of the community in the current time point
            tmp_size = len(community_set[item[0]])
            if item[1] * 1.0 / community_size > self.split_threshold:
                if item[1] * 1.0 / tmp_size > self.keep_threshold:
                    self.split_details.append(item[0])
            else:
                break

        if len(self.split_details) > 1:
            #pdb.set_trace()
            ok = True
        else:
            self.split_details = [] 

        return ok 
    
    def is_merged(self, community_set, vertex_dict, label):
        d = {}
        for vname in community_set[label]:
            #There are some node just appeared
            if vname not in self.last_vertex_dict:
                continue
            last_label = vertex_dict[vname].last_label
            if last_label not in d:
                d[last_label] = 0
            d[last_label] += 1
        #size = sum(d.values())
        community_size = len(community_set[label])
        l = sorted(d.items(), key=lambda item : item[1], reverse=True) 
        ok = False
        self.merged_details = [] 
        for item in l:
            #The size of the community in the last time point.
            tmp_size = len(self.last_community_set[item[0]])
            if item[1] * 1.0 / community_size > self.merged_threshold:
                if item[1] * 1.0 / tmp_size > self.keep_threshold:
                    self.merged_details.append(item[0])
            else:
                break
        if len(self.merged_details) > 1:
            ok = True
        else:
            self.merged_details = [] 

        return ok
 
    def reserve_community_set(self, community_set, vertex_dict):
        self.last_community_set = community_set
        self.last_vertex_dict = vertex_dict
    
    def detect_increment_community(self, ct):
        label_need_to_update = {}
        for v in self.g.vs:
            if v["name"] in self.new_node:
                v["label"] = self.generate_new_label()
                v["last_label"] = -1
            else:
                v["last_label"] = v["label"]
            #record the label of changed node
            if v["name"] in self.changed_node:
                label_need_to_update[v["last_label"]] = -1

        for v in self.g.vs:
            if v["last_label"] in label_need_to_update:
                self.active_list[v] = "active"
                
        self.llp()
        self.analyze_evolution(ct)
        #self.reserve_community_set()

    def detect_static_community(self, sd):
        self.get_init_graph(sd)

        self.active_list = {}
        for v in self.g.vs:
            self.active_list[v] = "active"

        self.llp()

    def track_community(self, sd, ct):
        self.get_init_graph(sd)
        ret = self.detect_init_community(ct)
        #self.get_correct_ratio(ret)
        ok = True
        increment_times = 9 
        i = 0
        print "\n"
        while i < increment_times:
            ok = self.update_graph(sd)
            ret = self.detect_increment_community(ct)
            i += 1
            print "\n"
        
        print ct.get_average_longevity()
        print ct.get_max_longevity()
        print ct.get_min_longevity()
        #ct.show_all_community_evolution()
        print ct.show_event_occur_probability()

def test_case1():
    #fname = "../../../panalog/aggregation.csv"
    fin_name = "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-17.csv"
    fout_name = "./tmp_ret/events_alpa.txt"

    window = 30 
    increment = 5

    sd = Stream_Data_Tiles3(fin_name)
    nm = Network_Measure(window, increment)
     
    ct = Community_Tracking(fout_name)
    
    #nm.change_rate(sd) 
    #nm.modularity(sd)
    t1 = time.time()
    nm.track_community(sd, ct)
    t2 = time.time()

    print "%.3f" % (t2 - t1)

def test_case2():
    ftopology = "/home/zhoumin/project/community tracking/opensource_data/email-Eu-core.txt"
    flabel = "/home/zhoumin/project/community tracking/opensource_data/email-Eu-core-department-labels.txt"
    
    gd = Graph_Data(ftopology)
    gd.get_label_dict(flabel)
    #gd.exist_singular_point()

    gd.detect_community()
    correct_ratio =  gd.get_correct_ratio()
    print "correct ratio: ", correct_ratio


def main():
    test_case1()

if __name__ == "__main__":
    main()
