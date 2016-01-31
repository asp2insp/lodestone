MAX_CHILDREN = 2
HISTORY = []

class BTree:
    def __init__(self):
        self.root = Node()

    def insert(self, value):
        HISTORY.append(self.root)
        self.root = self.root.insert(value)

    def __unicode__(self):
        return unicode(self.root)


class Node:
    id = 0
    def __init__(self):
        self.id = Node.id
        Node.id += 1
        self.values = []
        self.children = []

    def to_string(self, depth=0):
        sub = ",\n{}".format(' '*depth*3).join(
            map(lambda x: x.to_string(depth+1), self.children))
        nl = "\n" if self.children else ""
        return "{0}({4}) V:{1}, C:[\n{0}{2}{3}{0}]".format(
            ' '*depth*3, self.values, sub, nl, self.id)

    def __unicode__(self):
        return self.to_string()

    def __str__(self):
        return self.to_string()

    def _is_leaf(self):
        return len(self.children) == 0

    def _find_child_index_for(self, value):
      for i in xrange(len(self.values)):
        if value < self.values[i]:
          return i
        elif i == len(self.values)-1:
          return i
        elif self.values[i+1] > value:
          return i+1

    def _clone(self):
        n = Node()
        n.values = [x for x in self.values]
        n.children = [x for x in self.children]
        return n

    def _split(self):
        return None

    def _mut_insert(self, value):
        self._mut_insert_non_full(value)
        if len(self.values) > MAX_CHILDREN:
            print "SPLIT ({})\n{}\n".format(self.id, unicode(_T))
            return self._split()
        return self

    def _mut_insert_non_full(self, value):
        for i in xrange(len(self.values)):
            if self.values[i] > value:
                self.values.insert(i, value)
                return
        # If we get to here, it's the largest item
        self.values.append(value)

    def insert(self, value):
        if self._is_leaf():
            return self._clone()._mut_insert(value)
        else:
            c = self._clone()
            i = self._find_child_index_for(value)
            c.children[i] = c.children[i].insert(value)
            return c


_T = BTree()
if __name__ == "__main__":
    while True:
        v = int(raw_input("insert> "))
        _T.insert(value=v)
        print unicode(_T)
