class Stream_Data_2(object):
    def __init__(self, fname):
        self.fp = open(fname, "r")
        self.label_dict = {}
        
    def get_data_segment(self, period):
        self.label_dict = {}
        line = self.fp.readline() 
        edge_list = [] 
        edge = self.get_edge(line)
        edge_list.append(edge)
        begin_time = self.get_time(line)
        time = begin_time 
        label = self.get_label(line)
        self.update_label_dict(edge, label)
        
        #print "begin time: ", begin_time
        while time < begin_time + period:
            line = self.fp.readline()
            if len(line) == 0:
                break
            edge = self.get_edge(line)
            edge_list.append(edge)
            time = self.get_time(line)
            label = self.get_label(line)
            self.update_label_dict(edge, label)
        #print "end time: ", time
            
            
        return edge_list

    def get_node_label(self, node_name):
        return max(self.label_dict[node_name], key=self.label_dict[node_name].get)
        
    def get_community_set(self):
        community_set_given = {}
        for node in self.label_dict:
            label = self.get_node_label(node)
            if label not in community_set_given:
                community_set_given[label] = []
            community_set_given[label].append(node)

        return community_set_given
    
    def get_edge(self, line):
        return line.split(",")[0 : 2]

    def get_time(self, line):
        return int(line.split(",")[9])

    def get_label(self, line):
        index = line.find("_")
        return line.strip()[index + 1 : ]

    def update_label_dict(self, edge, label):
        if edge[0] not in self.label_dict:
            self.label_dict[edge[0]] = {}
        if label not in self.label_dict[edge[0]]:
            self.label_dict[edge[0]][label] = 0
        self.label_dict[edge[0]][label] += 1

        if edge[1] not in self.label_dict:
            self.label_dict[edge[1]] = {}
        if label not in self.label_dict[edge[1]]:
            self.label_dict[edge[1]][label] = 0
        self.label_dict[edge[1]][label] += 1

    def close(self):
        self.fp.close()
