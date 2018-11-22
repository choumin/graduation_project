class Stream_Data_3(object):
    def __init__(self, fname):
        self.fp = open(fname, "r")
        self.label_dict = {}
        self.line_list = self.get_sorted_line()
        self.cursor = 0
        self.line_count = len(self.line_list)

    def get_sorted_line(self):
        l = []
        line = self.fp.readline()
        while line:
            time = self.get_time(line)
            l.append([time, line])
            line = self.fp.readline()
        print "edge count: ", len(l)

        return sorted(l, key=lambda item:item[0])
        
    def readline(self):
        line = ""
        if self.cursor < self.line_count:
            line = self.line_list[self.cursor][1]
            self.cursor += 1

        return line

    def is_invalid(self, line):
        if line.find(",") == -1:
            return True 
        if len(line.split(",")) < 10:
            return True

        return False

    def get_data_segment(self, period):
        self.label_dict = {}
        line = self.readline() 
        edge_list = [] 
        if self.is_invalid(line):
            return edge_list    
        edge = self.get_edge(line)
        edge_list.append(edge)
        begin_time = self.get_time(line)
        time = begin_time 
        label = self.get_label(line)
        self.update_label_dict(edge, label)
        
        #print "begin time: ", begin_time
        while time < begin_time + period:
            line = self.readline()
            if self.is_invalid(line):
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
        #try:
        #    return int(line.split(",")[9])
        #except:
        #    print "error line: ", line
        #    return -1

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
