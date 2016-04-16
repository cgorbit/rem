from rem_logging import logger as logging

# ordered by 'value'
# lookupable by 'key'

class PriorityQueue(object):
    def __init__(self):
        getattr(super(PriorityQueue, self), "__init__")()
        self.revIndex = {}
        self.objects = []
        self.values = []

    @classmethod
    def _heapify(cls, objects, values):
        heap = cls()
        assert len(objects) == len(values)
        heap.objects = objects[:]
        heap.values = values[:]
        for i in xrange(len(heap) / 2, -1, -1):
            heap._pushdown(i)
        for i, o in enumerate(heap.objects):
            heap.revIndex[o] = i
        return heap

    def copy(self):
        return self._heapify(self.objects[:], self.values[:])

    def _swap(self, i, j):
        if i != j:
            self.objects[i], self.objects[j] = self.objects[j], self.objects[i]
            self.values[i], self.values[j] = self.values[j], self.values[i]
            if self.revIndex:
                self.revIndex[self.objects[i]] = i
                self.revIndex[self.objects[j]] = j

    def _pushdown(self, i):
        n = len(self.objects)
        while 2 * i + 1 < n:
            child = 2 * i + 1
            if 2 * i + 2 < n and self.values[2 * i + 2] < self.values[child]: child = 2 * i + 2
            if self.values[i] <= self.values[child]:
                break
            self._swap(i, child)
            i = child
        return i

    def _rollup(self, i):
        while i > 0:
            parent = (i - 1) / 2
            if self.values[parent] <= self.values[i]:
                break
            self._swap(i, parent)
            i = parent
        return i

    def add(self, key, value):
        if self.revIndex.has_key(key):
            logging.warning("%r already is in heap", key)
            self._change_value(key, value)
            return
        self.objects.append(key)
        self.values.append(value)
        pos = len(self.objects) - 1
        self.revIndex[key] = pos
        self._rollup(pos)

    # obsolete
    def pop(self, key=None):
        self._check_not_empty()
        idx = self.revIndex.get(key, None) if key else 0
        if idx is None:
            return
        return self._pop(idx)

    def pop_front(self):
        self._check_not_empty()
        return self._pop(0)

    def pop_by_key(self, key):
        idx = self.revIndex.get(key)
        if idx is None:
            raise KeyError()
        return self._pop(idx)

    def get(self, key, default=None):
        idx = self.revIndex.get(key)
        if idx is None:
            return default
        return self.values[idx]

    def _pop(self, idx):
        n = len(self.objects)
        self._swap(idx, n - 1)
        retObject = self.objects.pop()
        retVal = self.values.pop()
        del self.revIndex[retObject]
        if n - 1 != idx:
            self._pushdown(self._rollup(idx))
        return retObject, retVal

    def _check_not_empty(self):
        if not self.values:
            raise IndexError("PriorityQueue is empty")

    def peak(self):
        self._check_not_empty()
        return self.objects[0], self.values[0]

    front = peak

    def _change_value(self, object, value):
        pos = self.revIndex.get(object, None)
        if value == 0:
            if pos is not None:
                self._swap(pos, len(self.objects) - 1)
                self.values.pop()
                self.objects.pop()
                del self.revIndex[object]
                if pos < len(self.objects):
                    self._pushdown(self._rollup(pos))
        elif pos is None:
            self.add(object, value)
        else:
            old = self.values[pos]
            self.values[pos] = value
            if value > old:
                self._pushdown(pos)
            elif value < old:
                self._rollup(pos)

    def __len__(self):
        return len(self.objects)

    def __nonzero__(self):
        return bool(self.objects)

    def __contains__(self, key):
        return key in self.revIndex

    def __iter__(self):
        return self.objects.__iter__()

    def keys(self):
        return self.revIndex.keys()

    def items(self):
        return zip(self.objects, self.values)

