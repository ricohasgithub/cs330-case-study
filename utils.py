
import heapq
import math
import json

from collections import defaultdict
from datetime import datetime, timedelta

class BaseMatcher:

    def __init__(self):
        self.map = RoadNetwork()
        self.drivers = read_drivers("./case_study/data/drivers.csv")
        self.passengers = read_passengers("./case_study/data/passengers.csv")
        # Stores nearest node for each driver -- eventually implement k-means clustering
        self.nearest_nodes = dict()

    def update_driver(self, id, time, lat, lon):
        self.drivers[id] = {"time": time,
                            "source_lat": lat, "source_lon": lon}
        
    def get_closest_nodes(self, lat, lon):
        # Linearly search through all vertices in self.map and see which one has the least distance 
        min_distance, nearest = float("inf"), None
        for node, neighbors in self.map.graph.items():
            distance = self.map.get_distance(node, lat, lon)
            if distance < min_distance:
                min_distance = distance
                nearest = node
        return nearest

    # Override if neccesary
    def complete_ride(self, driver, passenger):
        # Find closest nodes to each of driver and passenger
        driver_node = self.get_closest_nodes(self.drivers[driver]["source_lat"], self.drivers[driver]["source_lon"]) if not driver in self.nearest_nodes.keys() else self.nearest_nodes[driver]
        passenger_node = self.get_closest_nodes(self.passengers[passenger]["source_lat"], self.passengers[passenger]["source_lon"])
        dest_node = self.get_closest_nodes(self.passengers[passenger]["dest_lat"], self.passengers[passenger]["dest_lon"])

        # Update the closest node to the driver to the passenger's destination node
        self.nearest_nodes[driver] = dest_node

        # Calculate driving time for driver to reach passenger
        time = self.map.get_time(driver_node, passenger_node)
        new_time = timedelta(hours=time) + max(self.drivers[driver]["time"], self.passengers[passenger]["time"])

        # Calculate driving time from passenger to their destination
        time = self.map.get_time(passenger_node, dest_node)
        new_time = timedelta(hours=time) + new_time

        # Update dictionary entry for driver time and position
        self.update_driver(driver, new_time, self.map.node_to_latlon[dest_node]["lat"], self.map.node_to_latlon[dest_node]["lon"])

    # Override implementation for each T_i algorithm
    # This method takes in a passenger and returns the "best" driver to match
    # with that passenger given some metric
    def match(self, availible_drivers, passenger_id):
        raise Exception("Not implemented")

class RoadNetwork:

    def __init__(self):
        # TODO: implement Floyd-Warshall to find shortest paths between all pairs of nodes
        self.graph, self.edge_data = read_adjacency("./case_study/data/adjacency.json")
        self.node_to_latlon = read_node_data("./case_study/data/node_data.json")

    def get_neighbors(self, u):
        return self.graph[u]
    
    def get_edge_data(self, u, v, query=None):
        return self.edge_data[(u, v)] if query == None else self.edge_data[(u, v)][query]
    
    # Get distance between a node and a coordinate
    def get_distance(self, u, lat, lon):
        lat_u, lon_u = self.node_to_latlon[u]["lat"], self.node_to_latlon[u]["lon"]
        # Return euclidean norm; assume we are on a locally flat plane
        return math.sqrt((lat_u - lat) ** 2 + (lon_u - lon) ** 2)
    
    def get_node_distance(self, u, v):
        lon_u, lon_v = self.node_to_latlon[u]["lon"], self.node_to_latlon[v]["lon"]
        lat_u, lat_v = self.node_to_latlon[u]["lat"], self.node_to_latlon[v]["lat"]
        # Return euclidean norm; assume we are on a locally flat plane
        return math.sqrt((lon_u - lon_v) ** 2 + (lat_u - lat_v) ** 2)
    
    # This method computes the shortest time needed for the driver to reach
    # a passenger at some (lat, lon) coord. Default implementation is A* with a euclidean heuristic
    def get_time(self, s, t):
        # We model the road network as a weighted graph where the edge weights are travel times
        # return the minimum shortest path for minimum time to go from s to t
        pq, dist = [(0, s)], dict()
        for node, _ in self.node_to_latlon.items():
            dist[node] = float("inf")
        dist[s] = 0

        while len(pq) > 0:
            cost, u = heapq.heappop(pq)
            if u == t:
                return dist[u]
            # Add all neighbors to the search queue
            for v in self.graph[u]:
                # We can still relax this edge
                if dist[v] > dist[u] + self.get_edge_data(u, v, "time"):
                    # dist[v] = dist[u] + w(u -> v)
                    dist[v] = dist[u] + self.get_edge_data(u, v, "time")
                    # Note that h is the euclidean distance, so we just call get_distance to t
                    v_cost = dist[v] + self.get_distance(t, 
                                                         self.node_to_latlon[v]["lat"],
                                                         self.node_to_latlon[v]["lon"])
                    heapq.heappush(pq, (v_cost, v))

        return dist[u]

# Read and parse adjacency.json as an adjacency list
def read_adjacency(path):
    graph = defaultdict(list)
    edge_data = defaultdict(dict)
    with open(path, "r") as file:
        data = json.load(file)
        for start_node_id, end_node_datum in data.items():
            for end_node_id, end_node_data in end_node_datum.items():
                # Build adjacency matrix view of graph edges
                graph[start_node_id].append(end_node_id)
                # Check to see if we have a multigraph
                if len(edge_data[(start_node_id, end_node_id)]) != 0:
                    print("Multigraph")
                # Build lookup table for edge data/weights
                edge_data[(start_node_id, end_node_id)] = end_node_data
    return graph, edge_data

# Read and parse node_data.json as a lookup table
def read_node_data(path):
    node_data = defaultdict(dict)
    with open(path, "r") as file:
        data = json.load(file)
        for id, lat_lon in data.items():
            # Each node's data comes in the form of id: {lon: ..., lat: ...}
            node_data[id] = lat_lon
    return node_data

# Read drivers.csv as a lookup table
def read_drivers(path):
    drivers = defaultdict(dict)
    # Read and parse the drivers.csv file
    with open(path, "r") as file:
        # Dummy variable for indexing the lookup table
        index = 0
        for line in file:
            if not line.startswith("Date/Time"):
                data = line.strip().split(",")
                date_time = datetime.strptime(data[0], "%m/%d/%Y %H:%M:%S")
                source_lat = float(data[1])
                source_lon = float(data[2])
                drivers[index] = {"time": date_time,
                                  "source_lat": source_lat, "source_lon": source_lon}
                index += 1
    return drivers

# Read passengers.csv as a lookup table
def read_passengers(path):
    passengers = defaultdict(dict)
    # Read and parse the passengers.csv file
    with open(path, "r") as file:
        # Dummy variable for indexing the lookup table
        index = 0
        for line in file:
            if not line.startswith("Date/Time"):
                data = line.strip().split(",")
                date_time = datetime.strptime(data[0], "%m/%d/%Y %H:%M:%S")
                source_lat = float(data[1])
                source_lon = float(data[2])
                dest_lat = float(data[3])
                dest_lon = float(data[4])
                passengers[index] = {"time": date_time,
                                     "source_lat": source_lat, "source_lon": source_lon,
                                     "dest_lat": dest_lat, "dest_lon": dest_lon}
                index += 1
    return passengers