from unionset import UnionSet

class Stream_Data_Tiles4(object):
    def __init__(self, fname):
        self.fp = open(fname, "r")
        #self.init_time = self.get_time(self.fp.readline())
        #self.fp.close()
        #self.fp = open(fname, "r")
        self.label_dict = {}
        self.line_list = self.get_sorted_line()
        #self.show_first_ten_lines()
        self.line_count = len(self.line_list)
        self.cursor = 0
        self.init_time = self.get_time(self.readline())
        self.cursor = 0
        self.node_dict = {}
        self.node_index = 0

    def __del__(self):
        self.close()

    def show_first_ten_lines(self):
        for i in range(0, 10):
            print self.line_list[i]

    def get_init_time(self):
        return self.init_time

    def get_one_edge(self):
        edge = []
        line = self.readline()
        if not line:
            return edge
        [u, v] = self.get_edge(line)
        if u not in self.node_dict:
            self.node_dict[u] = str(self.node_index)
            self.node_index += 1

        if v not in self.node_dict:
            self.node_dict[v] = str(self.node_index)
            self.node_index += 1

        edge.append(self.node_dict[u])
        edge.append(self.node_dict[v])
        edge.append(self.get_time(line))

        return edge
    
    def get_one_edge_inner(self, line):
        edge = []
        #line = self.readline()
        if not line:
            return edge
        [u, v] = self.get_edge(line)
        if u not in self.node_dict:
            self.node_dict[u] = str(self.node_index)
            self.node_index += 1

        if v not in self.node_dict:
            self.node_dict[v] = str(self.node_index)
            self.node_index += 1

        ui = self.node_dict[u]
        vi = self.node_dict[v]

        if ui not in self.neighbor_dict:
            self.neighbor_dict[ui] = []
        self.neighbor_dict[ui].append(vi)

        if vi not in self.neighbor_dict:
            self.neighbor_dict[vi] = []
        self.neighbor_dict[vi].append(ui)

        edge.append(self.node_dict[u])
        edge.append(self.node_dict[v])
        edge.append(self.get_time(line))

        return edge
        
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

    def is_label_valid(self, label, edge):
        u = edge[0]
        v = edge[1]
        if u not in self.label_dict:
            return True
        if v not in self.label_dict:
            return True
        if label in self.label_dict[u]:
            return True
        if label in self.label_dict[v]:
            return True

        return False

    def get_data_segment2(self, period):
        self.label_dict = {}
        self.neighbor_dict = {}
        line = self.readline() 
        self.edge_list = [] 
        if self.is_invalid(line):
            return self.edge_list    

        edge = self.get_one_edge_inner(line)
        label = self.get_label(line)
        self.edge_list.append(edge)
        begin_time = self.get_time(line)
        time = begin_time 
        self.update_label_dict(edge, label)
        
        #print "begin time: ", begin_time
        while time < begin_time + period:
            line = self.readline()
            if self.is_invalid(line):
                break
            edge = self.get_one_edge_inner(line)
            label = self.get_label(line)
            if self.is_label_valid(label, edge):
                self.edge_list.append(edge)
                time = self.get_time(line)
                self.update_label_dict(edge, label)
        #print "end time: ", time
            
        return self.edge_list

    def get_data_segment(self, period):
        self.label_dict = {}
        self.neighbor_dict = {}
        line = self.readline() 
        self.edge_list = [] 
        if self.is_invalid(line):
            return self.edge_list    
        edge = self.get_one_edge_inner(line)

        self.edge_list.append(edge)
        begin_time = self.get_time(line)
        time = begin_time 
        label = self.get_label(line)
        self.update_label_dict(edge, label)
        
        #print "begin time: ", begin_time
        while time < begin_time + period:
            line = self.readline()
            if self.is_invalid(line):
                break
            edge = self.get_one_edge_inner(line)
            self.edge_list.append(edge)
            time = self.get_time(line)
            label = self.get_label(line)
            self.update_label_dict(edge, label)
        #print "end time: ", time
            
        return self.edge_list

    def get_node_label(self, node_name):
        return max(self.label_dict[node_name], key=self.label_dict[node_name].get)
        
    def generate_flag_community(self, community):
        flag_community = []
        for node in community:
            flag_community.append([node, True])

        return flag_community
            
    def same_community(self, v, community):
        for node in community:
            if v in self.neighbor_dict[node]:
                return True
        return False

    def copy_community(self, community):
        c = []
        for node in community:
            c.append(node)

        return c

    def get_subgraphs(self, node_set):
        u = UnionSet()
        for node1 in node_set:
            u.init(node1)
            for node2 in self.neighbor_dict[node1]:
                if node2 in node_set:
                    u.init(node2)
                    u.join(node1, node2)

        subgraphs = []
        tmp_dict = {}
        for node in node_set:
            c = u.find(node)
            if c not in tmp_dict:
                tmp_dict[c] = []
            tmp_dict[c].append(node)
        for c in tmp_dict:
            subgraphs.append(tmp_dict[c])

        return subgraphs

    def get_community_set(self):
        community_dict = {}
        for node in self.label_dict:
            for label in self.label_dict[node]:
                if label not in community_dict:
                    community_dict[label] = []
                community_dict[label].append(node)

        communities = {}
        i = 0
        for label in community_dict:
            subgraphs = self.get_subgraphs(community_dict[label])
            for subgraph in subgraphs:
                communities[i] = subgraph
                i += 1

        return communities

    def get_community_set3(self):
        subgraphs = self.get_subgraphs(self.label_dict)
        communities = {}
        i = 0
        for subgraph in subgraphs:
            community_dict = {}
            for node in self.label_dict:
                label = self.get_node_label(node)
                if label not in community_dict:
                    community_dict[label] = []
                community_dict[label].append(node)
            for label in community_dict:
                communities[i] = community_dict[label]
                i += 1

        return communities

    def get_community_set2(self):
        community_set_given = {}
        for node in self.label_dict:
            label = self.get_node_label(node)
            if label not in community_set_given:
                community_set_given[label] = []
            community_set_given[label].append(node)

        communities = {}
        i = 0
        
        for label in community_set_given:
            tmp_community = self.copy_community(community_set_given[label])
            #flag_community = self.generate_flag_community(community_set_given[label])
            
            while len(tmp_community) > 0:
                community = []
                node = tmp_community[0]
                community.append(node)
                tmp_community.remove(node)
                ok = True
                while ok:
                    ok = False
                    for v in tmp_community:
                        if self.same_community(v, community):
                            ok = True
                            break
                    if ok == True:
                        community.append(v)
                        tmp_community.remove(v)
                #communities.append(community)
                communities[i] = community
                i += 1

        return communities
    
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

    def get_average_label_count(self):
        total_count = 0
        count = 0
        for node in self.label_dict:
            total_count += len(self.label_dict[node])
            count += 1

        return total_count * 1.0 / count

    def get_multi_label_ratio(self):
        total_count = 0
        count = 0
        for node in self.label_dict:
            if len(self.label_dict[node]) > 1:
                count += 1
            total_count += 1
        
        return count * 1.0 / total_count

    def get_star_details(self):
        sorted_nodes = sorted(self.neighbor_dict, key=lambda item : len(self.neighbor_dict[item]), reverse=True)
        a = 0
        b = 0
        for node1 in sorted_nodes:
            label1 = self.get_node_label(node1)
            total_count = 0
            count = 0
            for node2 in self.neighbor_dict[node1]:
                label2 = self.get_node_label(node2)
                if label1 == label2:
                    count += 1
                total_count += 1
            #print total_count, count * 1.0 / total_count
            ratio = count * 1.0 / total_count
            if ratio >= 0.8:
                a += 1
            b += 1
        print a * 1.0 / b

    def reset_cursor(self):
        self.cursor = 0

    def close(self):
        self.fp.close()
    

def main():
    upline = 60
    fname = "/home/zhoumin/project/community tracking/labeled_log0202/2018-01-17.csv"
    sd = Stream_Data_Tiles4(fname)
    for period in range(1, upline):
        #sd = Stream_Data_Tiles4(fname)
        edge_list = sd.get_data_segment(period * 60)
        #count = sd.get_average_label_count()
        #ratio = sd.get_multi_label_ratio()
        #print ratio
        sd.get_star_details()
        print "abcdefg"
        sd.reset_cursor()

if __name__ == "__main__":
    main()
