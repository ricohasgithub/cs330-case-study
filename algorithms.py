
import heapq
import math
import time
import random
import multiprocessing
from kd_tree import KDTree, Node  

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
            # print(f"PASSENGER CLOSEST Execution time: {execution_time} seconds")

            execution_time = 0
            for i in range(len(availible_drivers)):

                start_time = time.time()
                driver = availible_drivers[i]
                driver_id = driver[1]
                
                driver_node = self.get_closest_nodes(self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]) if not driver_id in self.nearest_nodes.keys() else self.nearest_nodes[driver_id]
                self.nearest_nodes[driver_id] = driver_node

                end_time = time.time()
                execution_time += end_time - start_time
                # print(f"CLOSEST Execution time: {execution_time} seconds")

                start_time = time.time()
                # Calculate starting drive hour
                hour = max(self.drivers[driver_id]["time"].hour, self.passengers[passenger_id]["time"].hour)
                # Calculate driving time for driver to reach passenger
                pickup_time = self.map.get_time(driver_node, passenger_node, hour, heuristic="djikstras")

                end_time = time.time()
                execution_time = end_time - start_time
                # print(f"DJIKSTRAS Execution time: {execution_time} seconds")

                if (pickup_time < min_time):
                    min_time = pickup_time
                    min_driver = i
            
            # print(f"DRIVER CLOSEST Execution time: {execution_time/len(availible_drivers)} seconds")
            driver_id = availible_drivers[min_driver][1]
            del availible_drivers[min_driver]

            start_time = time.time()
            driver_return_to_road = self.complete_ride(driver_id, passenger_id)
            end_time = time.time()
            execution_time = end_time - start_time
            # print(f"complete_ride Execution time: {execution_time} seconds")
        else:
            driver_id = availible_drivers[0][1]
            del availible_drivers[0]

            start_time = time.time()
            driver_return_to_road = self.complete_ride(driver_id, passenger_id)
            end_time = time.time()
            execution_time = end_time - start_time
            # print(f"complete_ride Execution time: {execution_time} seconds")
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

    # Get distance between a node and a coordinate
    def get_euclidean_distance(self, lat1, lon1, lat2, lon2):
        # Return euclidean norm; assume we are on a locally flat plane
        return math.sqrt((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2)

    # Binary search implementation
    def get_closest_nodes(self, lat, lon):

        low, high = 0, len(self.sorted_nodes) - 1
        nearest = None
        min_distance = float("inf")

        while low <= high:
            mid = (low + high) // 2
            current_node, _ = self.sorted_nodes[mid]  # Extracting the node from the tuple

            distance = self.map.get_distance(
                current_node,
                lat,
                lon
            )

            if distance < min_distance:
                min_distance = distance
                nearest = current_node

            if lat < self.map.node_to_latlon[current_node]['lat'] or (
                lat == self.map.node_to_latlon[current_node]['lat'] and lon < self.map.node_to_latlon[current_node]['lon']
            ):
                high = mid - 1
            else:
                low = mid + 1

        # Find a 50-neighbor radius for to find a local optima
        lower_bound = max(0, mid - 25)
        upper_bound = min(len(self.sorted_nodes), mid + 25)
        lon_nearest = nearest

        for i in range(lower_bound, upper_bound):
            current_node, _ = self.sorted_nodes[i]
            c_dist = self.get_euclidean_distance(lat, lon,
                                            self.map.node_to_latlon[current_node]["lat"],
                                            self.map.node_to_latlon[current_node]["lon"])
            if c_dist < min_distance:
                min_distance = c_dist
                lon_nearest = current_node

        return lon_nearest

    # Get best driver for a given passenger by finding first availible driver
    def match(self, availible_drivers, passenger_id):

        # Get the closest available driver by euclidean distance
        min_time = float("inf")
        min_driver_node = None
        min_driver = None

        # Minor optimization since if there's only 1 driver availible, then we don't need to check the pickup time
        if len(availible_drivers) != 1:

            start_time = time.time()
            # Find closest nodes to each of driver and passenger
            passenger_node = self.get_closest_nodes(self.passengers[passenger_id]["source_lat"], self.passengers[passenger_id]["source_lon"])
            end_time = time.time()
            execution_time = end_time - start_time
            # print(f"PASSENGER CLOSEST Execution time: {execution_time} seconds")

            passenger_lat, passenger_lon = self.passengers[passenger_id]["source_lat"], self.passengers[passenger_id]["source_lon"]

            # Sort all drivers by euclidean distance to passgner
            availible_drivers = sorted(availible_drivers, key=lambda x: self.get_euclidean_distance(
                                       passenger_lat, passenger_lon,
                                       self.drivers[x[1]]["source_lat"],
                                       self.drivers[x[1]]["source_lon"]))

            execution_time = 0
            for i in range(min(5, len(availible_drivers))):
                start_time = time.time()
                
                driver = availible_drivers[i]
                driver_id = driver[1]
                
                driver_node = self.get_closest_nodes(self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]) if driver_id not in self.nearest_nodes.keys() else self.nearest_nodes[driver_id]
                self.nearest_nodes[driver_id] = driver_node

                end_time = time.time()
                execution_time += end_time - start_time
                # print(f"DRIVER CLOSEST Execution time: {execution_time} seconds")

                # Calculate starting drive hour
                hour = max(self.drivers[driver_id]["time"].hour, self.passengers[passenger_id]["time"].hour)
                # Calculate driving time for driver to reach passenger
                pickup_time = self.map.get_time(driver_node, passenger_node, hour)

                if (pickup_time < min_time):
                    min_time = pickup_time
                    min_driver = i
                    min_driver_node = driver_node
                
                if pickup_time <= 0.1:
                    break

            # print(f"AVG DRIVER CLOSEST Execution time: {execution_time/len(availible_drivers)} seconds")
            driver_id = availible_drivers[min_driver][1]
            del availible_drivers[min_driver]

            start_time = time.time()
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, pickup_time=pickup_time)
            end_time = time.time()
            execution_time = end_time - start_time
            # print(f"complete_ride Execution time: {execution_time} seconds")
        else:
            driver_id = availible_drivers[0][1]
            del availible_drivers[0]

            start_time = time.time()
            driver_return_to_road = self.complete_ride(driver_id, passenger_id)
            end_time = time.time()
            execution_time = end_time - start_time
            # print(f"complete_ride Execution time: {execution_time} seconds")


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
        
        node_coordinates = [(self.map.node_to_latlon[node]['lat'], self.map.node_to_latlon[node]['lon']) for node, _ in self.sorted_nodes]

        self.kd_tree = KDTree(node_coordinates)

    def get_closest_nodes(self, lat, lon):
        nearest_point = self.kd_tree.closest_point((lat, lon))
        return nearest_point.point if nearest_point else None

        # # Sort coordinates by x-coordinate
        # self.sorted_coordinates = sorted(self.map.node_to_latlon.items(), key=lambda x: x[1]["lon"])
        
        # # Assuming latlon dictionaries have 'lat' and 'lon' keys
        # self.sorted_nodes = sorted(
        #     self.map.graph.items(),
        #     key=lambda item: (self.map.node_to_latlon[item[0]]['lat'], self.map.node_to_latlon[item[0]]['lon'])
        # )

    # def get_euclidean_distance(self, lat1, lon1, lat2, lon2):
    #     return math.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2)

    # def get_closest_nodes(self, lat, lon):
    #     low, high = 0, len(self.sorted_nodes) - 1
    #     nearest = None
    #     min_distance = float("inf")

    #     while low <= high:
    #         mid = (low + high) // 2
    #         current_node, _ = self.sorted_nodes[mid]  # Extracting the node from the tuple

    #         distance = self.map.get_distance(
    #             current_node,
    #             lat,
    #             lon
    #         )

    #         if distance < min_distance:
    #             min_distance = distance
    #             nearest = current_node

    #         if lat < self.map.node_to_latlon[current_node]['lat'] or (
    #             lat == self.map.node_to_latlon[current_node]['lat'] and lon < self.map.node_to_latlon[current_node]['lon']
    #         ):
    #             high = mid - 1
    #         else:
    #             low = mid + 1

    #     # Find a 100-neighbor radius for to find a local optima
    #     lower_bound = max(0, mid - 50)
    #     upper_bound = min(len(self.sorted_nodes), mid + 50)
    #     lon_nearest = nearest

    #     for i in range(lower_bound, upper_bound):
    #         current_node, _ = self.sorted_nodes[i]
    #         c_dist = self.get_euclidean_distance(lat, lon,
    #                                         self.map.node_to_latlon[current_node]["lat"],
    #                                         self.map.node_to_latlon[current_node]["lon"])
    #         if c_dist < min_distance:
    #             min_distance = c_dist
    #             lon_nearest = current_node

    #     return lon_nearest

    # Get best driver for a given passenger by finding first availible driver
    def match(self, availible_drivers, passenger_id):

        # Get the closest available driver by euclidean distance
        min_time = float("inf")
        min_driver_node = None
        min_driver = None

        # Minor optimization since if there's only 1 driver availible, then we don't need to check the pickup time
        if len(availible_drivers) != 1:

            start_time = time.time()
            # Find closest nodes to each of driver and passenger
            passenger_node = self.get_closest_nodes(self.passengers[passenger_id]["source_lat"], self.passengers[passenger_id]["source_lon"])
            end_time = time.time()
            execution_time = end_time - start_time
            # print(f"PASSENGER CLOSEST Execution time: {execution_time} seconds")

            execution_time = 0
            for i in range(len(availible_drivers)):

                start_time = time.time()
                
                driver = availible_drivers[i]
                driver_id = driver[1]
                
                driver_node = self.get_closest_nodes(self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]) if driver_id not in self.nearest_nodes.keys() else self.nearest_nodes[driver_id]
                self.nearest_nodes[driver_id] = driver_node

                end_time = time.time()
                execution_time += end_time - start_time
                # print(f"DRIVER CLOSEST Execution time: {execution_time} seconds")

                # Calculate starting drive hour
                hour = max(self.drivers[driver_id]["time"].hour, self.passengers[passenger_id]["time"].hour)
                # Calculate driving time for driver to reach passenger
                pickup_time = self.map.get_time(driver_node, passenger_node, hour, heuristic="manhattan")

                if (pickup_time < min_time):
                    min_time = pickup_time
                    min_driver = i
                    min_driver_node = driver_node

            # print(f"AVG DRIVER CLOSEST Execution time: {execution_time/len(availible_drivers)} seconds")
            driver_id = availible_drivers[min_driver][1]
            del availible_drivers[min_driver]

            start_time = time.time()
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, pickup_time=pickup_time, heuristic="manhattan")
            end_time = time.time()
            execution_time = end_time - start_time
            # print(f"complete_ride Execution time: {execution_time} seconds")
        else:
            driver_id = availible_drivers[0][1]
            del availible_drivers[0]

            start_time = time.time()
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, heuristic="manhattan")
            end_time = time.time()
            execution_time = end_time - start_time
            # print(f"complete_ride Execution time: {execution_time} seconds")


        if driver_return_to_road:
            heapq.heappush(self.drivers_pq, (self.drivers[driver_id]["time"],
                                            driver_id,
                                            self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]))


    
    # Get best driver for a given passenger by finding first availible driver
    def match_parallel(self, availible_drivers, passenger_id):

        # Get the closest available driver by euclidean distance
        min_time = float("inf")
        min_driver_node = None
        min_driver = None

        # Minor optimization since if there's only 1 driver availible, then we don't need to check the pickup time
        if len(availible_drivers) != 1:

            start_time = time.time()
            # Find closest nodes to each of driver and passenger
            passenger_node = self.get_closest_nodes(self.passengers[passenger_id]["source_lat"], self.passengers[passenger_id]["source_lon"])
            end_time = time.time()
            execution_time = end_time - start_time
            # print(f"PASSENGER CLOSEST Execution time: {execution_time} seconds")

            execution_time = 0
            for i in range(len(availible_drivers)):

                start_time = time.time()
                
                driver = availible_drivers[i]
                driver_id = driver[1]
                
                driver_node = self.get_closest_nodes(self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]) if driver_id not in self.nearest_nodes.keys() else self.nearest_nodes[driver_id]
                self.nearest_nodes[driver_id] = driver_node

                end_time = time.time()
                execution_time += end_time - start_time
                # print(f"DRIVER CLOSEST Execution time: {execution_time} seconds")

                # Calculate starting drive hour
                hour = max(self.drivers[driver_id]["time"].hour, self.passengers[passenger_id]["time"].hour)
                # Calculate driving time for driver to reach passenger
                pickup_time = self.map.get_time(driver_node, passenger_node, hour, heuristic="manhattan")

                if (pickup_time < min_time):
                    min_time = pickup_time
                    min_driver = i
                    min_driver_node = driver_node

            # print(f"AVG DRIVER CLOSEST Execution time: {execution_time/len(availible_drivers)} seconds")
            driver_id = availible_drivers[min_driver][1]
            del availible_drivers[min_driver]

            start_time = time.time()
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, pickup_time=pickup_time, heuristic="manhattan")
            end_time = time.time()
            execution_time = end_time - start_time
            # print(f"complete_ride Execution time: {execution_time} seconds")
        else:
            driver_id = availible_drivers[0][1]
            del availible_drivers[0]

            start_time = time.time()
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, heuristic="manhattan")
            end_time = time.time()
            execution_time = end_time - start_time
            # print(f"complete_ride Execution time: {execution_time} seconds")


        if driver_return_to_road:
            heapq.heappush(self.drivers_pq, (self.drivers[driver_id]["time"],
                                            driver_id,
                                            self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]))
