class UnionSet(object):
    def __init__(self):
        self.parent = {}

    def init(self, key):
        if key not in self.parent:
            self.parent[key] = key

    def find(self, key):
        self.init(key)
        while self.parent[key] != key:
            self.parent[key] = self.parent[self.parent[key]]
            key = self.parent[key]
        return key

    def join(self, key1, key2):
        p1 = self.find(key1)
        p2 = self.find(key2)
        if p1 != p2:
            self.parent[p2] = p1

if __name__ == '__main__':
    u = UnionSet()
    l1 = [1,2,3]
    l2 = [4,5,6]
    for i,j in zip(l1,l2):
        u.init(i)
        u.init(j)
        u.join(i,j)

    u.join(3,4)

    for i in range(1, 7):
        print(u.find(i))