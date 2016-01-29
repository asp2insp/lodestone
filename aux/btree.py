MAX_CHILDREN = 2

class BTree:
    def __init__(self):
        self.root = Node()

    def insert(self, key=None, value=None):
        mid, sib = self.root.insert(key, value)
        if mid:
            old_root = self.root
            self.root = Node()
            self.root.children = [old_root, sib]
            self.root.values.append(mid)

    def __unicode__(self):
        return unicode(self.root)

class Node:
    id = 0
    def __init__(self):
        # self.keys = []
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

    def is_leaf(self):
        return len(self.children) == 0

    def _find_child_for(self, value):
      for i in xrange(len(self.values)):
        if value < self.values[i]:
          return self.children[i]
        elif i == len(self.values)-1:
          return self.children[-1]
        elif self.values[i+1] > value:
          return self.children[i+1]


    def insert(self, key=None, value=None):
        # -> mid, Node
        mid, sib = (None, None)
        if self.is_leaf():
          self._insert_non_full(key, value)
          if len(self.values) > MAX_CHILDREN:
              print "SPLIT ({})\n{}\n".format(self.id, unicode(_T))
              return self._split()
        else:
          mid, sib = self._find_child_for(value).insert(value=value)
          if mid:
              self._insert_non_full(value=mid)
              self._insert_child(mid, sib)
              if len(self.values) > MAX_CHILDREN:
                print "SPLIT_PROPAGATE ({})\n{}\n".format(self.id, unicode(_T))
                return self._split()
        return None, None

    def _insert_child(self, key, node):
        for i in xrange(len(self.values)):
            if self.values[i] > key:
                self.children.insert(i, node)
                return
        # If we get to here it's the largest item
        self.children.append(node)

    def _insert_non_full(self, key=None, value=None):
        for i in xrange(len(self.values)):
            if self.values[i] > value:
                self.values.insert(i, value)
                return
            if self.values[i] == value:
                # TODO: add key/val here
                return
        # If we get to here, it's the largest item
        self.values.append(value)

    def _split(self):
        # -> mid, Node
        mid_i = len(self.values) // 2
        mid_val = self.values[mid_i]

        right = Node()
        mid_val_i = mid_i if self.is_leaf() else mid_i+1
        right.values.extend(self.values[mid_val_i:])
        right.children.extend(self.children[mid_i+1:])

        self.values = self.values[:mid_i]
        self.children = self.children[:mid_i+1]

        return mid_val, right


_T = BTree()
if __name__ == "__main__":
    while True:
        v = int(raw_input("insert> "))
        _T.insert(value=v)
        print unicode(_T)
