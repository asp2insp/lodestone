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
        sub = ",\n{}".format(' '*depth*2).join(
            map(lambda x: x.to_string(depth+1), self.children))
        nl = "\n" if self.children else ""
        return "{0}({4}) V:{1}, C:[\n{0}{2}{3}{0}]".format(
            ' '*depth*2, self.values, sub, nl, self.id)

    def __unicode__(self):
        return self.to_string()

    def is_leaf(self):
        return len(self.children) == 0

    def insert(self, key=None, value=None):
        # -> mid, Node
        mid, sib = (None, None)
        if self.is_leaf():
            self._insert_inner(key, value)
            if len(self.values) > MAX_CHILDREN:
                return self._split()
        else:
            for i in xrange(len(self.values)):
                if value < self.values[i] or i == len(self.values)-1:
                    mid, sib = self.children[i].insert(value=value)
                    if mid:
                        self._insert_child(mid, sib)
                        self._insert_inner(value=mid)
                        if len(self.values) > MAX_CHILDREN:
                            return self._split()
                    #     else:
                    #         return None, None
                    # else:
                    #     return None, None
        return None, None

    def _insert_child(self, key, node):
        for i in xrange(len(self.values)):
            if self.values[i] > key:
                self.children.insert(i, node)
                return
        # If we get to here it's the largest item
        self.children.append(node)

    def _insert_inner(self, key=None, value=None):
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

        right = Node()
        right.values.extend(self.values[mid_i:])
        right.children.extend(self.children[mid_i:])

        self.values = self.values[:mid_i]
        self.children = self.children[:mid_i]

        return right.values[0], right


if __name__ == "__main__":
    bt = BTree()

    while True:
        v = int(raw_input("insert> "))
        bt.insert(value=v)
        print unicode(bt)

    # # Root
    # r = Node()
    # r.values.extend([5,7])
    #
    # # Level a
    # a1 = Node()
    # a1.values.extend([3])
    # a2 = Node()
    # a2.values.extend([6])
    # a3 = Node()
    # a3.values.extend([10])
    #
    # r.children.extend([a1, a2, a3])
    #
    # # Level b
    # b1 = Node()
    # b1.values.extend([1,2])
    # b2 = Node()
    # b2.values.extend([3,4])
    # b3 = Node()
    # b3.values.extend([5])
    # b4 = Node()
    # b4.values.extend([6])
    # b5 = Node()
    # b5.values.extend([7,8])
    # b6 = Node()
    # b6.values.extend([10,11])
    #
    # a1.children.extend([b1,b2])
    # a2.children.extend([b3,b4])
    # a3.children.extend([b5,b6])
    #
    # print "{}\n".format(unicode(r))
    #
    # b6.insert(value=11)
    # print "{}\n".format(unicode(r))
