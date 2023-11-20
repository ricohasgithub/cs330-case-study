
import heapq
import math
import time
import random
import multiprocessing
from kd_tree import Node, build_kd_tree, find_nearest

from utils import *

class T1_Matcher(BaseMatcher):

    def __init__(self):
        super(T1_Matcher, self).__init__()
        '''
            Create a heap to store all drivers and passengers by time
            Note that the longest waiting passenger is just the passenger
            with the lowest add time. Since passengers are in sorted order
            and there are no mutation operations for modifying passenger
            join time, we do not need a priority queue for passengers
            (We can simply delete them directly once a ride is fulfilled)
        '''
        self.drivers_pq = []
        for id, data in self.drivers.items():
            # Insert time first so that heap sorts from min to max time
            heapq.heappush(self.drivers_pq, (data["time"], id,
                                             data["source_lat"], data["source_lon"]))
    
    # Get best driver for a given passenger by finding first availible driver
    def match(self, availible_drivers, passenger_id):
        # Get the first driver availible
        start_time, driver_id, _, _ = availible_drivers.popleft()
        # Process driver pick up and drop off; also get whether the driver returns for more rides
        driver_return_to_road = self.complete_ride(driver_id, passenger_id, heuristic="djikstras")
        
        if driver_return_to_road:
            # Re-queue into priority queue with end time of drop off and end position
            heapq.heappush(self.drivers_pq, (self.drivers[driver_id]["time"],
                                             driver_id,
                                             self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]))

class T2_Matcher(BaseMatcher):

    def __init__(self):
        super(T2_Matcher, self).__init__()
        '''
            Create a heap to store all drivers and passengers by time
            Note that the longest waiting passenger is just the passenger
            with the lowest add time. Since passengers are in sorted order
            and there are no mutation operations for modifying passenger
            join time, we do not need a priority queue for passengers
            (We can simply delete them directly once a ride is fulfilled)
        '''
        self.drivers_pq = []
        for id, data in self.drivers.items():
            # Insert time first so that heap sorts from min to max time
            heapq.heappush(self.drivers_pq, (data["time"], id,
                                             data["source_lat"], data["source_lon"]))

    # Get distance between a node and a coordinate
    def get_euclidean_distance(self, lat1, lon1, lat2, lon2):
        # Return euclidean norm; assume we are on a locally flat plane
        return math.sqrt((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2)
    
    # Get best driver for a given passenger by finding first availible driver
    def match(self, availible_drivers, passenger_id):

        # Get the closest available driver by euclidean distance
        min_distance = float("inf")
        min_driver = None
        
        for i in range(len(availible_drivers)):
            driver = availible_drivers[i]
            dist = self.get_euclidean_distance(self.passengers[passenger_id]["source_lat"], self.passengers[passenger_id]["source_lon"], driver[2], driver[3])
            if (dist < min_distance):
                min_distance = dist
                min_driver = i

        driver_id = availible_drivers[min_driver][1]
        del availible_drivers[min_driver]
        driver_return_to_road = self.complete_ride(driver_id, passenger_id, heuristic="djikstras")

        if driver_return_to_road:
            heapq.heappush(self.drivers_pq, (self.drivers[driver_id]["time"],
                                            driver_id,
                                            self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]))    

class T3_Matcher(BaseMatcher):

    def __init__(self):
        super(T3_Matcher, self).__init__()
        '''
            Create a heap to store all drivers and passengers by time
            Note that the longest waiting passenger is just the passenger
            with the lowest add time. Since passengers are in sorted order
            and there are no mutation operations for modifying passenger
            join time, we do not need a priority queue for passengers
            (We can simply delete them directly once a ride is fulfilled)
        '''
        self.drivers_pq = []
        for id, data in self.drivers.items():
            # Insert time first so that heap sorts from min to max time
            heapq.heappush(self.drivers_pq, (data["time"], id,
                                             data["source_lat"], data["source_lon"]))

    # Get distance between a node and a coordinate
    def get_euclidean_distance(self, lat1, lon1, lat2, lon2):
        # Return euclidean norm; assume we are on a locally flat plane
        return math.sqrt((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2)
    
    # Get best driver for a given passenger by finding first availible driver
    def match(self, availible_drivers, passenger_id):

        # Get the closest available driver by euclidean distance
        min_time = float("inf")
        min_driver = None

        # Minor optimization since if there's only 1 driver availible, then we don't need to check the pickup time
        if len(availible_drivers) != 1:

            # Find closest nodes to each of driver and passenger
            passenger_node = self.get_closest_nodes(self.passengers[passenger_id]["source_lat"], self.passengers[passenger_id]["source_lon"])
            for i in range(len(availible_drivers)):

                driver = availible_drivers[i]
                driver_id = driver[1]
                
                driver_node = self.get_closest_nodes(self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]) if not driver_id in self.nearest_nodes.keys() else self.nearest_nodes[driver_id]
                self.nearest_nodes[driver_id] = driver_node

                # Calculate starting drive hour
                if self.drivers[driver_id]["time"].day < self.passengers[passenger_id]["time"].day:
                    hour = self.passengers[passenger_id]["time"].hour
                elif self.drivers[driver_id]["time"].day > self.passengers[passenger_id]["time"].day:
                    hour = self.drivers[driver_id]["time"].hour
                else:
                    hour = max(self.drivers[driver_id]["time"].hour, self.passengers[passenger_id]["time"].hour)
                
                start_time = time.time()
                pickup_time = self.map.get_time(driver_node, passenger_node, hour)
                end_time = time.time()
                self.get_shortest_path_total_time += (end_time - start_time)
                self.get_shortest_path_total_calls += 1

                if (pickup_time < min_time):
                    min_time = pickup_time
                    min_driver = i
            
            driver_id = availible_drivers[min_driver][1]
            del availible_drivers[min_driver]
            driver_return_to_road = self.complete_ride(driver_id, passenger_id)
        else:
            driver_id = availible_drivers[0][1]
            del availible_drivers[0]
            driver_return_to_road = self.complete_ride(driver_id, passenger_id)
        
        if driver_return_to_road:
            heapq.heappush(self.drivers_pq, (self.drivers[driver_id]["time"],
                                            driver_id,
                                            self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]))

class T4_Matcher(BaseMatcher):

    def __init__(self):
        super(T4_Matcher, self).__init__()
        '''
            Create a heap to store all drivers and passengers by time
            Note that the longest waiting passenger is just the passenger
            with the lowest add time. Since passengers are in sorted order
            and there are no mutation operations for modifying passenger
            join time, we do not need a priority queue for passengers
            (We can simply delete them directly once a ride is fulfilled)
        '''
        self.drivers_pq = []
        for id, data in self.drivers.items():
            # Insert time first so that heap sorts from min to max time
            heapq.heappush(self.drivers_pq, (data["time"], id,
                                             data["source_lat"], data["source_lon"]))
        
        # Assuming latlon dictionaries have 'lat' and 'lon' keys
        self.sorted_nodes = sorted(
                    self.map.graph.items(),
                    key=lambda item: (self.map.node_to_latlon[item[0]]['lat'], self.map.node_to_latlon[item[0]]['lon'])
                )
        node_coordinates = [((self.map.node_to_latlon[node]['lat'], self.map.node_to_latlon[node]['lon']), node) for node, _ in self.sorted_nodes]
        self.kd_tree = build_kd_tree(node_coordinates)

    def get_closest_nodes(self, lat, lon):
        # Start timing current procedure
        start_time = time.time()
        nearest_node_id = find_nearest(self.kd_tree, (lat, lon)).id
        # Compute total time spent finding nearest node
        end_time = time.time()
        self.get_closest_total_time += (end_time - start_time)
        self.get_closest_total_calls += 1
        return nearest_node_id

    # Get distance between a node and a coordinate
    def get_euclidean_distance(self, lat1, lon1, lat2, lon2):
        # Return euclidean norm; assume we are on a locally flat plane
        return math.sqrt((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2)
    
    # # Get best driver for a given passenger by finding first availible driver
    def match(self, availible_drivers, passenger_id):

        # Get the closest available driver by euclidean distance
        min_time = float("inf")
        min_driver = None

        # Minor optimization since if there's only 1 driver availible, then we don't need to check the pickup time
        if len(availible_drivers) != 1:

            # Find closest nodes to each of driver and passenger
            passenger_node = self.get_closest_nodes(self.passengers[passenger_id]["source_lat"], self.passengers[passenger_id]["source_lon"])

            for i in range(len(availible_drivers)):
                
                driver = availible_drivers[i]
                driver_id = driver[1]
                
                driver_node = self.get_closest_nodes(self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]) if driver_id not in self.nearest_nodes.keys() else self.nearest_nodes[driver_id]
                self.nearest_nodes[driver_id] = driver_node

                # Calculate starting drive hour
                if self.drivers[driver_id]["time"].day < self.passengers[passenger_id]["time"].day:
                    hour = self.passengers[passenger_id]["time"].hour
                elif self.drivers[driver_id]["time"].day > self.passengers[passenger_id]["time"].day:
                    hour = self.drivers[driver_id]["time"].hour
                else:
                    hour = max(self.drivers[driver_id]["time"].hour, self.passengers[passenger_id]["time"].hour)
                
                # Calculate driving time for driver to reach passenger
                start_time = time.time()
                pickup_time = self.map.get_time(driver_node, passenger_node, hour)
                end_time = time.time()
                self.get_shortest_path_total_time += (end_time - start_time)
                self.get_shortest_path_total_calls += 1

                if (pickup_time < min_time):
                    min_time = pickup_time
                    min_driver = i

            driver_id = availible_drivers[min_driver][1]
            del availible_drivers[min_driver]
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, pickup_time=pickup_time)
        else:
            driver_id = availible_drivers[0][1]
            del availible_drivers[0]
            driver_return_to_road = self.complete_ride(driver_id, passenger_id)

        if driver_return_to_road:
            heapq.heappush(self.drivers_pq, (self.drivers[driver_id]["time"],
                                            driver_id,
                                            self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]))

class T5_Matcher(BaseMatcher):

    def __init__(self):
        super(T5_Matcher, self).__init__()
        '''
            Create a heap to store all drivers and passengers by time
            Note that the longest waiting passenger is just the passenger
            with the lowest add time. Since passengers are in sorted order
            and there are no mutation operations for modifying passenger
            join time, we do not need a priority queue for passengers
            (We can simply delete them directly once a ride is fulfilled)
        '''
        self.drivers_pq = []
        for id, data in self.drivers.items():
            # Insert time first so that heap sorts from min to max time
            heapq.heappush(self.drivers_pq, (data["time"], id,
                                             data["source_lat"], data["source_lon"]))
        self.sorted_nodes = sorted(
                    self.map.graph.items(),
                    key=lambda item: (self.map.node_to_latlon[item[0]]['lat'], self.map.node_to_latlon[item[0]]['lon'])
                )
        node_coordinates = [((self.map.node_to_latlon[node]['lat'], self.map.node_to_latlon[node]['lon']), node) for node, _ in self.sorted_nodes]
        self.kd_tree = build_kd_tree(node_coordinates)

    def get_closest_nodes(self, lat, lon):
        # Start timing current procedure
        start_time = time.time()
        nearest_node_id = find_nearest(self.kd_tree, (lat, lon)).id
        # Compute total time spent finding nearest node
        end_time = time.time()
        self.get_closest_total_time += (end_time - start_time)
        self.get_closest_total_calls += 1
        return nearest_node_id
    
    # Get distance between a node and a coordinate
    def get_euclidean_distance(self, lat1, lon1, lat2, lon2):
        # Return euclidean norm; assume we are on a locally flat plane
        return math.sqrt((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2)

    # Get best driver for a given passenger by finding first availible driver
    def match(self, availible_drivers, passenger_id):

        # Get the closest available driver by euclidean distance
        min_time = float("inf")
        min_driver = None

        # Minor optimization since if there's only 1 driver availible, then we don't need to check the pickup time
        if len(availible_drivers) != 1:

            # Find closest nodes to each of driver and passenger
            passenger_node = self.get_closest_nodes(self.passengers[passenger_id]["source_lat"], self.passengers[passenger_id]["source_lon"])
            passenger_lat, passenger_lon = self.passengers[passenger_id]["source_lat"], self.passengers[passenger_id]["source_lon"]

            # Sort all drivers by euclidean distance to passgner
            availible_drivers.sort(key=lambda x: self.get_euclidean_distance(
                                       passenger_lat, passenger_lon,
                                       self.drivers[x[1]]["source_lat"],
                                       self.drivers[x[1]]["source_lon"]))

            # Candidate pool; prune all candidates outside the 10 closest by euclidean distance            
            candidates = [(availible_drivers[i], i) for i in range(min(10, len(availible_drivers)))]
            # Prioritize candidates with earlier log-on times
            candidates.sort(key=lambda x: self.drivers[x[0][1]]["time"])

            for i in range(len(candidates)):
                
                driver = candidates[i][0]
                driver_index = candidates[i][1]
                driver_id = driver[1]
                
                driver_node = self.get_closest_nodes(self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]) if driver_id not in self.nearest_nodes.keys() else self.nearest_nodes[driver_id]
                self.nearest_nodes[driver_id] = driver_node

                 # Calculate starting drive hour
                if self.drivers[driver_id]["time"].day < self.passengers[passenger_id]["time"].day:
                    hour = self.passengers[passenger_id]["time"].hour
                elif self.drivers[driver_id]["time"].day > self.passengers[passenger_id]["time"].day:
                    hour = self.drivers[driver_id]["time"].hour
                else:
                    hour = max(self.drivers[driver_id]["time"].hour, self.passengers[passenger_id]["time"].hour)
                
                # Calculate driving time for driver to reach passenger
                start_time = time.time()
                pickup_time = self.map.get_time(driver_node, passenger_node, hour)
                if (driver_node, passenger_node) not in self.past_times:
                    self.past_times[(driver_node, passenger_node)] = pickup_time
                end_time = time.time()
                self.get_shortest_path_total_time += (end_time - start_time)
                self.get_shortest_path_total_calls += 1

                if (pickup_time < min_time):
                    min_time = pickup_time
                    min_driver = driver_index

                # Check to see if we can make a match that gaurantees that the driver can
                # pick up the passenger in 10 minutes or less
                if pickup_time <= 0.1:
                    break

            driver_id = availible_drivers[min_driver][1]
            del availible_drivers[min_driver]
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, pickup_time=pickup_time, heuristic="manhattan")
        else:
            driver_id = availible_drivers[0][1]
            del availible_drivers[0]
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, heuristic="manhattan")
            
        if driver_return_to_road:
            heapq.heappush(self.drivers_pq, (self.drivers[driver_id]["time"],
                                            driver_id,
                                            self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]))