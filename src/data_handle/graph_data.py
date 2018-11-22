class Graph_Data(object):
    def __init__(self, fname):
        self.label_index = 0
        fp = open(fname, "r")
        line = fp.readline()
        self.g = Graph()
        self.node_dict = {}
        self.edge_dict = {}
        self.community_set = {}
        self.active_list = {} 
        self.coefficient = 1
        while line:
            edge = self.get_edge(line)
            if edge[0] not in self.node_dict:
                self.g.add_vertex(edge[0])
                self.node_dict[edge[0]] = -1

            if edge[1] not in self.node_dict:
                self.g.add_vertex(edge[1])
                self.node_dict[edge[1]] = -1
            
            self.g.add_edge(edge[0], edge[1])

            #edge_key1 = edge[0] + "_" + edge[1]
            #edge_key2 = edge[1] + "_" + edge[0]

            #if edge_key1 not in self.edge_dict and edge_key2 not in self.edge_dict:
            #    self.g.add_edge(edge[0], edge[1])
            
            #self.edge_dict[edge_key1] = -1
            #self.edge_dict[edge_key2] = -1

            line = fp.readline()

        fp.close()

        print self.g.summary()
    
    def generate_new_label(self):
        self.label_index += 1
        return self.label_index

    def get_edge(self, line):
        edge = self.get_vector(line)
        if len(edge) != 2:
            print "invalid edge"
            exit()

        return edge

    def get_key_value(self, line):
        key_value = self.get_vector(line)
        if len(key_value) != 2:
            print "invalid key-value"
            exit()

        return key_value 

    def get_vector(self, line):
        return [item.strip() for item in line.strip().split(' ')]

    def get_label_dict(self, fname):
        fp = open(fname, "r")
        line = fp.readline()
        self.label_dict = {}
        while line:
            key_value = self.get_key_value(line)
            if key_value[1] not in self.label_dict:
                self.label_dict[key_value[1]] = []
            self.label_dict[key_value[1]].append(key_value[0])
            #self.label_dict[]
            #self.label_dict[key_value[0]] = key_value[1]
            line = fp.readline()

        fp.close()

    def exist_singular_point(self):
        count = 0
        for v in self.g.vs:
            if v.degree() == 0:
                count += 1
        print "singular point count: ", count
        
    def detect_community(self):
        for v in self.g.vs:
            v["label"] = self.generate_new_label()
            v["tmp_label"] = -1 
        #self.modified_lpa()
        self.llp()

    def get_correct_ratio(self):
        #for each community detected by our method to match the most similar community in ground-truth.
        #get community by using our method.
        #get community in ground-truth
        self.get_community_set()
        print "community count in our method: ", len(self.community_set)
        print "community count in ground-truth: ", len(self.label_dict)
        total_ratio = 0.0
        for label1 in self.community_set:
            max_ratio = 0.0
            max_label = 0
            for label2 in self.label_dict:
                tmp_ratio = self.get_similarity(self.community_set[label1], self.label_dict[label2])
                if tmp_ratio > max_ratio:
                    max_ratio = tmp_ratio
                    max_label = label2
            print max_ratio, len(self.community_set[label1]), len(self.label_dict[max_label]),\
                    len(set(self.community_set[label1]) & set(self.label_dict[max_label]))
            total_ratio += max_ratio
            #print max_ratio,
        print "\n"

        return total_ratio / len(self.community_set)

    def get_similarity(self, v1, v2):
        s1 = set(v1)
        s2 = set(v2)

        #return len(s1 & s2) * 1.0 / len(s1 | s2)
        return len(s1 & s2) * 1.0 / len(s2)
        #return len(s1 & s2) * 1.0 / len(s1)
            

    def modified_lpa(self):
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
        
        print "iteration_count: " + str(i)

    def get_community_set(self):
        for v in self.g.vs:
            if v["label"] not in self.community_set:
                self.community_set[v["label"]] = []
            self.community_set[v["label"]].append(v["name"])

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
        for v in self.g.vs:
            #self.active_list.append(v)
            self.active_list[v] = "active" 
        #tmp_active_list = {}
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
