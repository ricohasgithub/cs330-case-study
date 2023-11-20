
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
        self.total_rides_completed = 0
        # Record total/cumulative times spent performing different parts of the algorithm
        self.get_closest_total_time = 0
        self.get_shortest_path_total_time = 0
        self.get_closest_total_calls = 0
        self.get_shortest_path_total_calls = 0
        self.past_times = dict()

    def update_driver(self, id, time, rides, lat, lon):
        self.drivers[id] = {"time": time, "rides": rides,
                            "source_lat": lat, "source_lon": lon}
    
    # Override if neccesary
    def get_closest_nodes(self, lat, lon):

        # Start timing current procedure
        start_time = time.time()

        # Linearly search through all vertices in self.map and see which one has the least distance 
        min_distance, nearest = float("inf"), None
        for node, neighbors in self.map.graph.items():
            distance = self.map.get_distance(node, lat, lon)
            if distance < min_distance:
                min_distance = distance
                nearest = node
        
        # Compute total time spent finding nearest node
        end_time = time.time()
        self.get_closest_total_time += (end_time - start_time)
        self.get_closest_total_calls += 1

        return nearest

    # Override if neccesary; run through the simulation of picking up and dropping off a passenger
    # returns True/False for if the driver is returning for more rides
    def complete_ride(self, driver, passenger, driver_node=None, passenger_node=None, pickup_time=None, heuristic="euclidean"):

        # Find closest nodes to each of driver and passenger
        if not driver_node:
            driver_node = self.get_closest_nodes(self.drivers[driver]["source_lat"], self.drivers[driver]["source_lon"]) if not driver in self.nearest_nodes.keys() else self.nearest_nodes[driver]
        if not passenger_node:
            passenger_node = self.get_closest_nodes(self.passengers[passenger]["source_lat"], self.passengers[passenger]["source_lon"])
        dest_node = self.get_closest_nodes(self.passengers[passenger]["dest_lat"], self.passengers[passenger]["dest_lon"])
        
        # Calculate starting drive hour; note that we check for the day in the case which
        # a driver logs in at 23h the night before, and the passenger is requesting a ride
        # the day after at an early time, (say at 0h or 1h)
        if self.drivers[driver]["time"].day < self.passengers[passenger]["time"].day:
            hour = self.passengers[passenger]["time"].hour
        elif self.drivers[driver]["time"].day > self.passengers[passenger]["time"].day:
            hour = self.drivers[driver]["time"].hour
        else:
            hour = max(self.drivers[driver]["time"].hour, self.passengers[passenger]["time"].hour)

        # Calculate driving time for driver to reach passenger
        if not pickup_time:
            start_time = time.time()
            pickup_time = self.map.get_time(driver_node, passenger_node, hour, heuristic=heuristic)
            end_time = time.time()
            self.get_shortest_path_total_time += (end_time - start_time)
            self.get_shortest_path_total_calls += 1
            # Cache results
            if (driver_node, passenger_node) not in self.past_times:
                self.past_times[(driver_node, passenger_node)] = pickup_time
        
        # Time to get to pickup location is start time + time to drive to pickup location
        new_time = timedelta(hours=pickup_time) + max(self.drivers[driver]["time"], self.passengers[passenger]["time"])

        # Calculate driving time from passenger to their destination
        start_time = time.time()
        driving_time = self.map.get_time(passenger_node, dest_node, hour, heuristic=heuristic)

        end_time = time.time()
        self.get_shortest_path_total_time += (end_time - start_time)
        self.get_shortest_path_total_calls += 1

        if (passenger_node, dest_node) not in self.past_times:
            self.past_times[(passenger_node, dest_node)] = driving_time
        
        # Start time at pickup location + time to drive to arrival location
        # So this is just dropoff time
        new_time = timedelta(hours=driving_time) + new_time

        # Update the closest node to the driver to the passenger's destination node
        self.nearest_nodes[driver] = dest_node

        # Final arrival time - passenger login time
        self.d1 += ((new_time - self.passengers[passenger]["time"]).total_seconds() / 60)
        self.d2 += (driving_time - pickup_time) * 60
        print("D1: ", (new_time - self.passengers[passenger]["time"]).total_seconds() / 60)
        print("D2: ", (driving_time - pickup_time) * 60)

        # Decrement the number of rides the driver has left before they are too exhausted
        rides = self.drivers[driver]["rides"] - 1
        self.total_rides_completed += 1

        if rides <= 0:
            # The driver has expended their "driver capacity", retire them
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
    
    def summarize_experiments(self):

        print("---------D1------------")
        print("Cumulative D1:", self.d1)
        print("Average D1:", self.d1 / self.total_rides_completed)

        print("---------D2------------")
        print("Cumulative D2:", self.d2)
        print("Average D2:", self.d2 / self.total_rides_completed)

        print("---------D3------------")
        print("Total time spent finding closest nodes:", self.get_closest_total_time)
        print("Average time spent finding closest nodes:", self.get_closest_total_time / self.get_closest_total_calls)
        print("Total time spent finding shortest paths:", self.get_shortest_path_total_time)
        print("Average time spent finding shortest paths:", self.get_shortest_path_total_time / self.get_shortest_path_total_calls)

class RoadNetwork:

    def __init__(self):
        self.graph, self.edge_data, self.speed_limit = read_adjacency("data/adjacency.json")
        self.node_to_latlon = read_node_data("data/node_data.json")
        
        # Used Only For B3
        self.traffic = {}

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
        pq, dist = [(0, s)], defaultdict(lambda: float("inf"))
        dist[s] = 0

        while pq:
            cost, u = heapq.heappop(pq)
            if u == t:
                return dist[u]
            # Add all neighbors to the search queue
            for v in self.graph[u]:
                new_dist = dist[u] + self.get_edge_data(u, v, hour, "time")
                # We can still relax this edge
                if dist[v] > new_dist:
                    dist[v] = new_dist
                    if heuristic == "euclidean":
                        # Note that h is the euclidean distance, so we just call get_distance to t
                        v_cost = dist[v] + self.get_distance(t, 
                                                            self.node_to_latlon[v]["lat"],
                                                            self.node_to_latlon[v]["lon"])
                    elif heuristic == "djikstras":
                        v_cost = dist[v]
                    elif heuristic == "manhattan":
                        v_cost = dist[v] + abs(self.node_to_latlon[t]["lat"] -
                                            self.node_to_latlon[v]["lat"]) + abs(self.node_to_latlon[t]["lon"] - self.node_to_latlon[v]["lon"])
                    heapq.heappush(pq, (v_cost, v))

        return dist[t]

    # This method computes the shortest time needed for the driver to reach including traffic.
    # a passenger at some (lat, lon) coord. Default implementation is A* with a euclidean heuristic
    def get_time_with_traffic(self, s, t, hour, heuristic="euclidean"):

        # We model the road network as a weighted graph where the edge weights are travel times
        # return the minimum shortest path for minimum time to go from s to t
        pq, dist, prev = [(0, s)], defaultdict(lambda: float("inf")), {}
        dist[s] = 0
        prev[s] = None

        while pq:
            cost, u = heapq.heappop(pq)
            if u == t:
                break  # Stop when the target is reached
            # Add all neighbors to the search queue
            for v in self.graph[u]:
                curr_path = self.get_edge_data(u, v, hour, "time")
                if (u, v) in self.traffic:
                    curr_path *= self.traffic[(u, v)]
                new_dist = dist[u] + curr_path
                # We can still relax this edge
                if dist[v] > new_dist:
                    dist[v] = new_dist
                    prev[v] = u  # Store the predecessor
                    if heuristic == "euclidean":
                        # Note that h is the euclidean distance
                        v_cost = dist[v] + self.get_distance(t, 
                                                            self.node_to_latlon[v]["lat"],
                                                            self.node_to_latlon[v]["lon"])
                    elif heuristic == "djikstras":
                        v_cost = dist[v]
                    elif heuristic == "manhattan":
                        v_cost = dist[v] + abs(self.node_to_latlon[t]["lat"] -
                                            self.node_to_latlon[v]["lat"]) + abs(self.node_to_latlon[t]["lon"] - self.node_to_latlon[v]["lon"])
                    heapq.heappush(pq, (v_cost, v))

        path = []
        u = t
        while prev[u] is not None:
            path.append((prev[u], u))
            u = prev[u]

        for u, v in path:
            self.traffic[(u, v)] = self.traffic.get((u, v), 0) + 1

        return dist[t], path  # Return the distance and the path

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
                # Compute a random driver capacity from around 10-12 rides
                drivers[index] = {"time": date_time, "rides": random.randint(7, 12),
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
