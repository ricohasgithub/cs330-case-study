
import heapq
import math
import json
import time
import random

from collections import defaultdict
from datetime import datetime, timedelta
import time as timer
import time as timer

class BaseMatcher:

    def __init__(self):
        self.map = RoadNetwork()
        self.drivers = read_drivers("data/drivers.csv")
        self.passengers = read_passengers("data/passengers.csv")
        # Stores nearest node for each driver
        self.nearest_nodes = dict()
        # Metrics to measure performance in alignment with desiderata
        self.d1 = 0
        self.d2 = 0

    def update_driver(self, id, time, rides, lat, lon):
        self.drivers[id] = {"time": time, "rides": rides,
                            "source_lat": lat, "source_lon": lon}
    
    # Override if neccesary
    def get_closest_nodes(self, lat, lon):
        # Linearly search through all vertices in self.map and see which one has the least distance 
        min_distance, nearest = float("inf"), None
        for node, neighbors in self.map.graph.items():
            distance = self.map.get_distance(node, lat, lon)
            if distance < min_distance:
                min_distance = distance
                nearest = node
        return nearest

    # Override if neccesary; run through the simulation of picking up and dropping off a passenger
    # returns True/False for if the driver is returning for more rides
    def complete_ride(self, driver, passenger, driver_node=None, passenger_node=None, heuristic="euclidean",):
        
        if not driver_node and not passenger_node:
            start_time = time.time()

        # Find closest nodes to each of driver and passenger
        if not driver_node:
            driver_node = self.get_closest_nodes(self.drivers[driver]["source_lat"], self.drivers[driver]["source_lon"]) if not driver in self.nearest_nodes.keys() else self.nearest_nodes[driver]
        if not passenger_node:
            passenger_node = self.get_closest_nodes(self.passengers[passenger]["source_lat"], self.passengers[passenger]["source_lon"])
        dest_node = self.get_closest_nodes(self.passengers[passenger]["dest_lat"], self.passengers[passenger]["dest_lon"])
        
        if not driver_node and not passenger_node:
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"CLOSEST Execution time: {execution_time} seconds")

        # Calculate starting drive hour
        hour = max(self.drivers[driver]["time"].hour, self.passengers[passenger]["time"].hour)

        start_time = time.time()

        # Calculate driving time for driver to reach passenger
        pickup_time = self.map.get_time(driver_node, passenger_node, hour, heuristic=heuristic)
        # Time to get to pickup location is start time + time to drive to pickup location
        new_time = timedelta(hours=pickup_time) + max(self.drivers[driver]["time"], self.passengers[passenger]["time"])

        # Calculate driving time from passenger to their destination
        driving_time = self.map.get_time(passenger_node, dest_node, hour, heuristic=heuristic)
        # Start time at pickup location + time to drive to arrival location
        # So this is just dropoff time
        new_time = timedelta(hours=driving_time) + new_time

        # Update the closest node to the driver to the passenger's destination node
        self.nearest_nodes[driver] = dest_node

        end_time = time.time()
        execution_time = end_time - start_time
        # print(f"A* Execution time: {execution_time} seconds")

        # Calculate starting drive hour
        hour = max(self.drivers[driver]["time"].hour, self.passengers[passenger]["time"].hour)

        # Final arrival time - passenger login time
        self.d1 += ((new_time - self.passengers[passenger]["time"]).total_seconds() / 60)
        self.d2 += (driving_time - pickup_time) * 60
        # print("D1: ", (new_time - self.passengers[passenger]["time"]).total_seconds() / 60)
        # print("D2: ", (driving_time - pickup_time) * 60)

        # Increment the number of rides the driver has completed
        rides = self.drivers[driver]["rides"] - 1
        if rides <= 0:
            # print("DRIVER RETIRED")
            return False
        else:
            # Update dictionary entry for driver time and position
            self.update_driver(driver, new_time, rides, self.map.node_to_latlon[dest_node]["lat"], self.map.node_to_latlon[dest_node]["lon"])
            return True

    # Override implementation for each T_i algorithm
    # This method takes in a passenger and returns the "best" driver to match
    # with that passenger given some metric
    def match(self, availible_drivers, passenger_id):
        raise Exception("Not implemented")

class RoadNetwork:

    def __init__(self):
        # TODO: implement Floyd-Warshall to find shortest paths between all pairs of nodes
        self.graph, self.edge_data, self.speed_limit = read_adjacency("data/adjacency.json")
        print(self.speed_limit)
        self.node_to_latlon = read_node_data("data/node_data.json")

    def get_neighbors(self, u):
        return self.graph[u]
    
    def get_edge_data(self, u, v, hour, query=None):
        return self.edge_data[(u, v)][hour] if query == None else self.edge_data[(u, v)][hour][query]

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
    def get_time(self, s, t, hour, heuristic="euclidean"):
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
                if dist[v] > dist[u] + self.get_edge_data(u, v, hour, "time"):
                    # dist[v] = dist[u] + w(u -> v)
                    dist[v] = dist[u] + self.get_edge_data(u, v, hour, "time")
                    if heuristic == "euclidean":
                        # Note that h is the euclidean distance, so we just call get_distance to t
                        v_cost = dist[v] + ((self.get_distance(t, 
                                                               self.node_to_latlon[v]["lat"],
                                                               self.node_to_latlon[v]["lon"])) / self.speed_limit)
                    elif heuristic == "djikstras":
                        v_cost = dist[v]
                    heapq.heappush(pq, (v_cost, v))
        return dist[u]

# Read and parse adjacency.json as an adjacency list
def read_adjacency(path):
    graph = defaultdict(list)
    edge_data = defaultdict(lambda: defaultdict(dict))
    max_speed = float("-inf")
    with open(path, "r") as file:
        data = json.load(file)
        for start_node_id, end_node_datum in data.items():
            for end_node_id, end_node_data in end_node_datum.items():
                # Build adjacency matrix view of graph edges
                graph[start_node_id].append(end_node_id)
                # Build lookup table for edge data/weights
                for hour_of_the_day_data in end_node_data:
                    max_speed = max(max_speed, hour_of_the_day_data["max_speed"])
                    hour = hour_of_the_day_data["hour"]
                    edge_data[(start_node_id, end_node_id)][hour] = hour_of_the_day_data
    print("Completed reading adjacency.json")
    return graph, edge_data, max_speed

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
                # Compute a random driver capacity from around 10-20 rides
                drivers[index] = {"time": date_time, "rides": random.randint(10, 20),
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
