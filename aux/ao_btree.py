MAX_CHILDREN = 2
HISTORY = []

class BTree:
    def __init__(self):
        self.root = Node()


    def insert(self, value):
        HISTORY.append(self.root)
        result = self.root.insert(value)
        if isinstance(result, Node):
            self.root = result
        else:
            mid, left, right = result
            self.root = Node()
            self.root.values.append(mid)
            self.root.children.extend([left, right])


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
          return len(self.values)
        elif self.values[i+1] > value:
          return i+1


    def _clone(self):
        n = Node()
        n.values = [x for x in self.values]
        n.children = [x for x in self.children]
        return n


    def _split(self):
        # -> (int, Node, Node)
        mid_i = len(self.values) // 2
        mid_val = self.values[mid_i]

        right = Node()
        mid_val_i = mid_i if self._is_leaf() else mid_i+1
        right.values.extend(self.values[mid_val_i:])
        right.children.extend(self.children[mid_i+1:])

        left = self._clone()
        left.values = left.values[:mid_i]
        left.children = left.children[:mid_i+1]

        return mid_val, left, right


    def _mut_insert(self, value):
        # -> Node|(int, Node, Node)
        self._mut_insert_non_full(value)
        if len(self.values) > MAX_CHILDREN:
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
        # -> Node|(int, Node, Node)
        if self._is_leaf():
            return self._clone()._mut_insert(value)
        else:
            c = self._clone()
            i = self._find_child_index_for(value)
            result = c.children[i].insert(value)
            if isinstance(result, Node):
                c.children[i] = result
            else:
                mid, left, right = result
                c._mut_insert_non_full(mid)
                c.children[i] = left
                c.children.insert(i+1, right)
                if len(c.values) > MAX_CHILDREN:
                  return c._split()
            return c


_T = BTree()
if __name__ == "__main__":
    while True:
        try:
          cmd = raw_input("insert|cmd> ")
        except EOFError:
          print "quitting..."
          break
        if cmd == "history":
          for t in HISTORY:
            print "HIST: {}".format(unicode(t))
          print "HIST: {}".format(unicode(_T))
        else:
          v = int(cmd)
          _T.insert(v)
          print unicode(_T)
