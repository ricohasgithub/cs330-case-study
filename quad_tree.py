
import math

class Point:

    def __init__(self, lat, lon, id):
        self.lat, self.lon = lat, lon
        self.id = id

    def get_distance(self, other):
        other_lat, other_lon = other.lat, other.lon
        return math.sqrt((self.lat - other_lat) ** 2 + (self.lon - other_lon) ** 2)

class Box:

    def __init__(self, c_lat, c_lon, w, h):
        self.c_lat, self.c_lon = c_lat, c_lon
        self.w, self.h = w, h
        self.west, self.east = c_lat - w/2, c_lat + w/2
        self.north, self.south = c_lon - h/2, c_lon + h/2

    def contains(self, point):
        return (point.lat >= self.west and
                point.lat <  self.east and
                point.lon >= self.north and
                point.lon < self.south)

    def intersects(self, other):
        return not (other.west > self.east or
                    other.east < self.west or
                    other.north > self.south or
                    other.south < self.north)

class QuadTree:

    def __init__(self, boundary, max_points=64, depth=0):
        self.boundary = boundary
        self.max_points = max_points
        self.points = []
        self.depth = depth
        self.divided = False

    def divide(self):
        c_lat, c_lon = self.boundary.c_lat, self.boundary.c_lon
        w, h = self.boundary.w / 2, self.boundary.h / 2
        # The boundaries of the four children nodes are "northwest",
        # "northeast", "southeast" and "southwest" quadrants within the
        # boundary of the current node.
        self.nw = QuadTree(Box(c_lat - w/2, c_lon - h/2, w, h),
                                    self.max_points, self.depth + 1)
        self.ne = QuadTree(Box(c_lat + w/2, c_lon - h/2, w, h),
                                    self.max_points, self.depth + 1)
        self.se = QuadTree(Box(c_lat + w/2, c_lon + h/2, w, h),
                                    self.max_points, self.depth + 1)
        self.sw = QuadTree(Box(c_lat - w/2, c_lon + h/2, w, h),
                                    self.max_points, self.depth + 1)
        self.divided = True

    def insert(self, point):
        if not self.boundary.contains(point):
            return False
        if len(self.points) < self.max_points:
            # There's room for our point without dividing the QuadTree
            self.points.append(point)
            return True
        # No room: divide if necessary, then try the sub-quads
        if not self.divided:
            self.divide()
        return (self.ne.insert(point) or
                self.nw.insert(point) or
                self.se.insert(point) or
                self.sw.insert(point))

    def query(self, boundary, found_points):

        if not self.boundary.intersects(boundary):
            # If the domain of this node does not intersect the search region, we don't need to look in it for points.
            return False

        # Search this node's points to see if they lie within boundary...
        for point in self.points:
            if boundary.contains(point):
                found_points.append(point)
        
        # ... and if this node has children, search them too
        if self.divided:
            self.nw.query(boundary, found_points)
            self.ne.query(boundary, found_points)
            self.se.query(boundary, found_points)
            self.sw.query(boundary, found_points)
        
        return found_points
    
    def find_closest_point(self, query_point):
        closest_point, distance = self._find_closest_point_recursive(self, query_point, float('inf'), None)
        return closest_point, distance

    def _find_closest_point_recursive(self, current_node, query_point, best_distance, closest_point):
        if current_node is None:
            return closest_point, best_distance

        # Check if the current node is a leaf
        if current_node.divided:
            # If the node is not a leaf, recursively search each child node
            for child in [current_node.nw, current_node.ne, current_node.se, current_node.sw]:
                closest_point, best_distance = self._find_closest_point_recursive(child, query_point, best_distance, closest_point)
        
        # Check this node's points to see if they are closer to the query point
        for point in current_node.points:
            distance = point.get_distance(query_point)
            if distance < best_distance:
                best_distance = distance
                closest_point = point

        return closest_point, best_distance