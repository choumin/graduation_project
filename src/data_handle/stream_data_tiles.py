class Stream_Data_Tiles(object):
    def __init__(self, fname):
        self.fp = open(fname, "r")
        self.init_time = self.get_time(self.fp.readline())
        self.fp.close()
        self.fp = open(fname, "r")
        self.label_dict = {}
        self.node_dict = {}
        self.node_index = 0
        
    def get_init_time(self):
        return self.init_time

    def get_one_edge(self):
        edge = []
        line = self.fp.readline()
        if not line:
            return edge
        [u, v] = self.get_edge(line)
        if u not in self.node_dict:
            self.node_dict[u] = self.node_index
            self.node_index += 1

        if v not in self.node_dict:
            self.node_dict[v] = self.node_index
            self.node_index += 1

        edge.append(self.node_dict[u])
        edge.append(self.node_dict[v])
        edge.append(self.get_time(line))

        return edge

    def get_data_segment(self, period):
        self.label_dict = {}
        line = self.fp.readline() 
        edge_list = [] 
        edge = self.get_edge(line)
        begin_time = self.get_time(line)
        edge.append(begin_time)
        edge_list.append(edge)
        time = begin_time 
        label = self.get_label(line)
        self.update_label_dict(edge, label)
        
        #print "begin time: ", begin_time
        while time < begin_time + period:
            line = self.fp.readline()
            if len(line) == 0:
                break
            edge = self.get_edge(line)
            time = self.get_time(line)
            edge.append(time)
            edge_list.append(edge)
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
