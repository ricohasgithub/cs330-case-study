
class Node:
    def __init__(self, point, id, left=None, right=None):
        self.point = point  # The point (x, y)
        self.id = id        # The node id
        self.left = left    # Left child
        self.right = right  # Right child

def build_kd_tree(points, depth=0):
    n = len(points)
    if n == 0:
        return None

    axis = depth % 2  # Alternate between x and y axis
    sorted_points = sorted(points, key=lambda point: point[0][axis])
    median_index = n // 2

    # Create a new node and construct subtrees
    node = Node(sorted_points[median_index][0], sorted_points[median_index][1])
    node.left = build_kd_tree(sorted_points[:median_index], depth + 1)
    node.right = build_kd_tree(sorted_points[median_index + 1:], depth + 1)
    return node

def find_nearest(node, point, depth=0, best=None):
    if node is None:
        return best

    axis = depth % 2
    next_best = None
    next_branch = None

    if best is None or distance_squared(point, node.point) < distance_squared(point, best.point):
        next_best = node
    else:
        next_best = best

    if point[axis] < node.point[axis]:
        next_branch = find_nearest(node.left, point, depth + 1, next_best)
    else:
        next_branch = find_nearest(node.right, point, depth + 1, next_best)

    if next_branch is None or distance_squared(point, next_best.point) < distance_squared(point, next_branch.point):
        return next_best
    else:
        return next_branch

def distance_squared(point1, point2):
    return (point1[0] - point2[0]) ** 2 + (point1[1] - point2[1]) ** 2
