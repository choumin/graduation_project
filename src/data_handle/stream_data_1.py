class Stream_Data_1(object):
    def __init__(self, fname):
        self.fp = open(fname)
        self.index = 0
        self.current_time = 0
        #self.interval = 10 * 60 
        #self.increment = 1 * 60
        self.cached = False
        self.cached_line = ""
        self.leap_year = [0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
        self.common_year = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
        
    def is_leap(self, year):
        if year % 4 == 0:
            if year % 100 == 0:
                if year % 400 == 0:
                    return True
                else:
                    return False
            else:
                return True
        else:
            return False

    def get_init_time(self):
        line = self.fp.readline()
        self.cached = True
        self.cached_line = line
        return self.get_time(line)

    def get_time_from_item(self, s):
        #pdb.set_trace() 
        [date, time] = s.split(" ")
        [year, month, day] = [int(item) for item in date.split("-")]
        [hour, minute, second] = [int(item) for item in time.split(":")]
        total_second = 0
        if self.is_leap(year):
            total_second = (self.leap_year[month] + day - 1) * 24 * 60 * 60 + hour * 60 * 60 + minute * 60 + second 
        else:
            total_second = (self.common_year[month] + day - 1) * 24 * 60 * 60 + hour * 60 * 60 + minute * 60 + second 

        return total_second 
        
    def get_time(self, line):
        array = line.strip().split(',')
        return self.get_time_from_item(array[2])

    def get_one_edge(self, line, begin_time, interval):
        #pdb.set_trace()
        #t = [item.strip() for item in line.split("\t")]
        edge = []
            
        array = line.strip().split(',')
        source = array[4]
        target = array[6]
        mytime = self.get_time_from_item(array[2])
        label = array[1]

        ok = False
        if (mytime - begin_time) < interval:
            edge.append(source)
            edge.append(target)
            edge.append(label)
            edge.append(mytime)
            edge.append(array[2])
            ok = True
        else:
            self.cached_line = line
            self.cached = True
        
        return ok, edge 
        
    def get_data_segment(self, begin_time, interval):
        data = []

        if self.cached:
            line = self.cached_line
            self.cached = False
        else:
            line = self.fp.readline()

        if not line:
            return data

        #self.current_time = self.get_time(line)
        ok, edge = self.get_one_edge(line, begin_time, interval)
        i = 0
        while ok:
            data.append(edge)
            line = self.fp.readline()
            if not line:
                break
            ok, edge = self.get_one_edge(line, begin_time, interval)
            i += 1
            #if i % 10000 == 0:
            #    print i, edge
                
        #print i
        
        return data

    def close(self):
        self.fp.close()
