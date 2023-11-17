class Node:
    def __init__(self, point, left=None, right=None):
        self.point = point
        self.left = left
        self.right = right

class KDTree:
    def __init__(self, points, depth=0):
        k = len(points[0])
        axis = depth % k

        if len(points) == 0:
            self.root = None
        else:
            points.sort(key=lambda x: x[axis])
            median = len(points) // 2
            self.root = Node(points[median])
            self.root.left = KDTree(points[:median], depth + 1)
            self.root.right = KDTree(points[median + 1:], depth + 1)

    def closest_point(self, target, depth=0, best=None):
        if self.root is None:
            return None

        k = len(target)
        axis = depth % k
        next_branch = None
        opposite_branch = None

        if target[axis] < self.root.point[axis]:
            next_branch = self.root.left
            opposite_branch = self.root.right
        else:
            next_branch = self.root.right
            opposite_branch = self.root.left

        best = self._closer_point(target, self.closest_point(target, depth + 1, best), best)

        if self._should_check_other_branch(target, best, axis):
            best = self._closer_point(target, opposite_branch.closest_point(target, depth + 1, best), best)

        return best

    def _closer_point(self, target, p1, p2):
        if p1 is None:
            return p2

        if p2 is None:
            return p1

        d1 = self._distance(target, p1.point)
        d2 = self._distance(target, p2.point)

        return p1 if d1 < d2 else p2

    def _distance(self, point1, point2):
        return sum((a - b) ** 2 for a, b in zip(point1, point2)) ** 0.5

    def _should_check_other_branch(self, target, best, axis):
        return best is None or abs(target[axis] - self.root.point[axis]) < best.point[axis]