
import heapq
import math
import time
import random

from utils import *
from quad_tree import *
from quad_tree import *

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
        driver_return_to_road = self.complete_ride(driver_id, passenger_id)
        
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
        driver_return_to_road = self.complete_ride(driver_id, passenger_id)

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

            start_time = time.time()
            # Find closest nodes to each of driver and passenger
            passenger_node = self.get_closest_nodes(self.passengers[passenger_id]["source_lat"], self.passengers[passenger_id]["source_lon"])
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"PASSNEGER CLOSEST Execution time: {execution_time} seconds")

            for i in range(len(availible_drivers)):

                start_time = time.time()
                driver = availible_drivers[i]
                driver_id = driver[1]
                driver_node = self.get_closest_nodes(self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]) if not driver_id in self.nearest_nodes.keys() else self.nearest_nodes[driver_id]
                self.nearest_nodes[driver_id] = driver_node

                end_time = time.time()
                execution_time = end_time - start_time
                print(f"CLOSEST Execution time: {execution_time} seconds")

                start_time = time.time()
                # Calculate starting drive hour
                hour = max(self.drivers[driver_id]["time"].hour, self.passengers[passenger_id]["time"].hour)
                # Calculate driving time for driver to reach passenger
                pickup_time = self.map.get_time(driver_node, passenger_node, hour, heuristic="djikstras")

                end_time = time.time()
                execution_time = end_time - start_time
                print(f"DJIKSTRAS Execution time: {execution_time} seconds")

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
        # Sort coordinates by x-coordinate
        self.sorted_coordinates = sorted(self.map.node_to_latlon.items(), key=lambda x: x[1]["lon"])
        self.nodes = list(self.map.graph.keys())
        self.sorted_nodes = sorted(self.nodes, key=lambda node: (node.lat, node.lon))

    def get_closest_node_divide_and_conquer(self, lat, lon):
        closest_node = self._closest_node_recursive(self.sorted_nodes, lat, lon)
        return closest_node

    def _closest_node_recursive(self, nodes, lat, lon):
        if len(nodes) <= 3:
            return min(nodes, key=lambda node: self.map.get_distance(node, lat, lon))

        mid = len(nodes) // 2
        mid_node = nodes[mid]

        left_nodes = nodes[:mid]
        right_nodes = nodes[mid:]

        left_closest = self._closest_node_recursive(left_nodes, lat, lon)
        right_closest = self._closest_node_recursive(right_nodes, lat, lon)

        closest_in_strip = self._closest_in_strip(nodes, mid_node, lat, lon, min(left_closest, right_closest))

        return min(left_closest, right_closest, closest_in_strip, key=lambda node: self.map.get_distance(node, lat, lon))

    def _closest_in_strip(self, nodes, mid_node, lat, lon, min_distance):
        strip = [node for node in nodes if mid_node.lat - min_distance <= node.lat <= mid_node.lat + min_distance]
        strip = sorted(strip, key=lambda node: node.lon)

        for i in range(len(strip)):
            for j in range(i+1, min(i+7, len(strip))):
                distance = self.map.get_distance(strip[i], lat, lon)
                if distance < min_distance:
                    min_distance = distance

        return min_distance

    
    # Get best driver for a given passenger by finding first availible driver
    def match(self, availible_drivers, passenger_id):

        # Get the closest available driver by euclidean distance
        min_time = float("inf")
        min_driver = None

        # Minor optimization since if there's only 1 driver availible, then we don't need to check the pickup time
        if len(availible_drivers) != 1:

            start_time = time.time()
            # Find closest nodes to each of driver and passenger
            passenger_node = self.get_closest_nodes(self.passengers[passenger_id]["source_lat"], self.passengers[passenger_id]["source_lon"])
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"PASSNEGER CLOSEST Execution time: {execution_time} seconds")

            for i in range(len(availible_drivers)):

                start_time = time.time()

                driver = availible_drivers[i]
                driver_id = driver[1]
                driver_node = self.get_closest_nodes(self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]) if not driver_id in self.nearest_nodes.keys() else self.nearest_nodes[driver_id]
                self.nearest_nodes[driver_id] = driver_node

                end_time = time.time()
                execution_time = end_time - start_time
                print(f"CLOSEST Execution time: {execution_time} seconds")

                start_time = time.time()

                # Calculate starting drive hour
                hour = max(self.drivers[driver_id]["time"].hour, self.passengers[passenger_id]["time"].hour)
                # Calculate driving time for driver to reach passenger
                pickup_time = self.map.get_time(driver_node, passenger_node, hour)

                end_time = time.time()
                execution_time = end_time - start_time
                print(f"A* Execution time: {execution_time} seconds")

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
    
    # Get best driver for a given passenger by finding first availible driver
    def match(self, availible_drivers, passenger_id):

        # Get the closest available driver by euclidean distance
        min_time = float("inf")
        min_driver = None

        # Minor optimization since if there's only 1 driver availible, then we don't need to check the pickup time
        if len(availible_drivers) != 1:

            start_time = time.time()
            # Find closest nodes to each of driver and passenger
            passenger_node = self.get_closest_nodes(self.passengers[passenger_id]["source_lat"], self.passengers[passenger_id]["source_lon"])
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"PASSNEGER CLOSEST Execution time: {execution_time} seconds")

            for i in range(len(availible_drivers)):

                start_time = time.time()

                driver = availible_drivers[i]
                driver_id = driver[1]
                driver_node = self.get_closest_nodes(self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]) if not driver_id in self.nearest_nodes.keys() else self.nearest_nodes[driver_id]
                self.nearest_nodes[driver_id] = driver_node

                end_time = time.time()
                execution_time = end_time - start_time
                print(f"CLOSEST Execution time: {execution_time} seconds")


                start_time = time.time()

                # Calculate starting drive hour
                hour = max(self.drivers[driver_id]["time"].hour, self.passengers[passenger_id]["time"].hour)
                # Calculate driving time for driver to reach passenger
                pickup_time = self.map.get_time(driver_node, passenger_node, hour)

                end_time = time.time()
                execution_time = end_time - start_time
                print(f"A* Execution time: {execution_time} seconds")

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
