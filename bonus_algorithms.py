
import heapq
import math
import time
import random
import multiprocessing
from datetime import datetime, timedelta
import time as timer
import time as timer
from kd_tree import Node, build_kd_tree, find_nearest

from utils import *


class B1_Matcher(BaseMatcher):

    def __init__(self):
        super(B1_Matcher, self).__init__()
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

    # Get distance between a node and a coordinate
    def get_euclidean_distance(self, lat1, lon1, lat2, lon2):
        # Return euclidean norm; assume we are on a locally flat plane
        return math.sqrt((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2)

    def get_closest_nodes(self, lat, lon):
        # Start timing current procedure
        start_time = time.time()
        nearest_node_id = find_nearest(self.kd_tree, (lat, lon)).id
        # Compute total time spent finding nearest node
        end_time = time.time()
        self.get_closest_total_time += (end_time - start_time)
        self.get_closest_total_calls += 1
        return nearest_node_id
    
    # overrides
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

        # Increment the number of rides the driver has completed
        blockedHours = set([16, 17, 18, 19, 20, 21, 22, 23, 24])
        if rides <= 0:
            if hour in blockedHours:
                return True
            # print("DRIVER RETIRED")
            return False
        else:
            # Update dictionary entry for driver time and position
            self.update_driver(driver, new_time, rides, self.map.node_to_latlon[dest_node]["lat"], self.map.node_to_latlon[dest_node]["lon"])
            return True


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

            for i in range(min(10, len(availible_drivers))):
                
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
                if (driver_node, passenger_node) not in self.past_times:
                    self.past_times[(driver_node, passenger_node)] = pickup_time
                end_time = time.time()
                self.get_shortest_path_total_time += (end_time - start_time)
                self.get_shortest_path_total_calls += 1

                if (pickup_time < min_time):
                    min_time = pickup_time
                    min_driver = i
                
                if pickup_time <= 0.1:
                    break

            driver_id = availible_drivers[min_driver][1]
            del availible_drivers[min_driver]
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, pickup_time=min_time, heuristic="manhattan")
        else:
            driver_id = availible_drivers[0][1]
            del availible_drivers[0]
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, heuristic="manhattan")
            
        if driver_return_to_road:
            heapq.heappush(self.drivers_pq, (self.drivers[driver_id]["time"],
                                            driver_id,
                                            self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]))


class B2_Matcher(BaseMatcher):
    def __init__(self):
        super(B2_Matcher, self).__init__()
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
        self.numDriverRides = {}

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
        min_mod_time = float("inf")
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

            for i in range(min(5, len(availible_drivers))):
                
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
                if (driver_node, passenger_node) not in self.past_times:
                    self.past_times[(driver_node, passenger_node)] = pickup_time
                end_time = time.time()
                self.get_shortest_path_total_time += (end_time - start_time)
                self.get_shortest_path_total_calls += 1

                numRides = self.numDriverRides.get(driver_id, 0)
                mod_pickup_time = pickup_time * (1.5 ** (numRides / 10 + 1))
                

                if (mod_pickup_time < min_mod_time):
                    min_time = pickup_time
                    min_mod_time = mod_pickup_time
                    min_driver = i
                
                if pickup_time <= 0.1:
                    break

            driver_id = availible_drivers[min_driver][1]
            del availible_drivers[min_driver]
            self.numDriverRides[driver_id] = self.numDriverRides.get(driver_id, 0) + 1
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, pickup_time=min_time, heuristic="manhattan")
        else:
            driver_id = availible_drivers[0][1]
            del availible_drivers[0]
            self.numDriverRides[driver_id] = self.numDriverRides.get(driver_id, 0) + 1
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, heuristic="manhattan")
            
        if driver_return_to_road:
            heapq.heappush(self.drivers_pq, (self.drivers[driver_id]["time"],
                                            driver_id,
                                            self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]))


class B2_Default_Matcher(BaseMatcher):

    def __init__(self):
        super(B2_Default_Matcher, self).__init__()
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
        self.numDriverRides = {}

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

            for i in range(min(10, len(availible_drivers))):
                
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
                if (driver_node, passenger_node) not in self.past_times:
                    self.past_times[(driver_node, passenger_node)] = pickup_time
                end_time = time.time()
                self.get_shortest_path_total_time += (end_time - start_time)
                self.get_shortest_path_total_calls += 1

                if (pickup_time < min_time):
                    min_time = pickup_time
                    min_driver = i
                
                if pickup_time <= 0.1:
                    break

            driver_id = availible_drivers[min_driver][1]
            del availible_drivers[min_driver]
            self.numDriverRides[driver_id] = self.numDriverRides.get(driver_id, 0) + 1
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, pickup_time=min_time, heuristic="manhattan")
        else:
            driver_id = availible_drivers[0][1]
            del availible_drivers[0]
            self.numDriverRides[driver_id] = self.numDriverRides.get(driver_id, 0) + 1
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, heuristic="manhattan")
            
        if driver_return_to_road:
            heapq.heappush(self.drivers_pq, (self.drivers[driver_id]["time"],
                                            driver_id,
                                            self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]))


class B3_Matcher(BaseMatcher):

    def __init__(self):
        super(B3_Matcher, self).__init__()
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
            availible_drivers.sort(key=lambda x: self.get_euclidean_distance(
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
                if self.drivers[driver_id]["time"].day < self.passengers[passenger_id]["time"].day:
                    hour = self.passengers[passenger_id]["time"].hour
                elif self.drivers[driver_id]["time"].day > self.passengers[passenger_id]["time"].day:
                    hour = self.drivers[driver_id]["time"].hour
                else:
                    hour = max(self.drivers[driver_id]["time"].hour, self.passengers[passenger_id]["time"].hour)
                # Calculate driving time for driver to reach passenger
                pickup_time, path = self.map.get_time_with_traffic(driver_node, passenger_node, hour)


                if (pickup_time < min_time):
                    min_time = pickup_time
                    min_driver = i
                    selected_path = path
                    min_driver_node = driver_node

                if pickup_time <= 0.1:
                    break

            # Add Best Path to Traffic
            self.map.add_traffic(selected_path, hour)
            
            # print(f"AVG DRIVER CLOSEST Execution time: {execution_time/len(availible_drivers)} seconds")
            driver_id = availible_drivers[min_driver][1]
            del availible_drivers[min_driver]

            start_time = time.time()
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, pickup_time=min_time)
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




class B4_Matcher(BaseMatcher):

    def __init__(self):
        super(B4_Matcher, self).__init__()
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
        node_coordinates = [(self.map.node_to_latlon[node]['lat'], self.map.node_to_latlon[node]['lon']) for node, _ in self.sorted_nodes]


    def get_euclidean_distance(self, lat1, lon1, lat2, lon2):
        return math.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2)

    def get_closest_nodes(self, lat, lon):

        # Start timing current procedure
        start_time = time.time()

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

        # Find a 100-neighbor radius for to find a local optima
        lower_bound = max(0, mid - 50)
        upper_bound = min(len(self.sorted_nodes), mid + 50)
        lon_nearest = nearest

        for i in range(lower_bound, upper_bound):
            current_node, _ = self.sorted_nodes[i]
            c_dist = self.get_euclidean_distance(lat, lon,
                                            self.map.node_to_latlon[current_node]["lat"],
                                            self.map.node_to_latlon[current_node]["lon"])
            if c_dist < min_distance:
                min_distance = c_dist
                lon_nearest = current_node

        # Compute total time spent finding nearest node
        end_time = time.time()
        self.get_closest_total_time += (end_time - start_time)
        self.get_closest_total_calls += 1

        return lon_nearest

    def complete_ride(self, driver, passenger, driver_node=None, passenger_node=None, pickup_time=None, heuristic="euclidean"):
        
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
            if (driver_node, passenger_node) not in self.past_times:
                pickup_time = self.map.get_time(driver_node, passenger_node, hour, heuristic=heuristic)
                print("pickup time not matched")
            else:
                pickup_time = self.past_times[(driver_node, passenger_node)]
                print("pickup time matched")
                self.match_counter += 1
            end_time = time.time()
            self.get_shortest_path_total_time += (end_time - start_time)
            self.get_shortest_path_total_calls += 1
        
        # Time to get to pickup location is start time + time to drive to pickup location
        new_time = timedelta(hours=pickup_time) + max(self.drivers[driver]["time"], self.passengers[passenger]["time"])

        # Calculate driving time from passenger to their destination
        start_time = time.time()
        if (passenger_node, dest_node) not in self.past_times:
            driving_time = self.map.get_time(passenger_node, dest_node, hour, heuristic=heuristic)
            print("destination time not matched")
        else:
            driving_time = self.past_times[(passenger_node, dest_node)]
            print("destination time matched")
            self.match_counter += 1

        end_time = time.time()
        self.get_shortest_path_total_time += (end_time - start_time)
        self.get_shortest_path_total_calls += 1
        
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

            for i in range(min(10, len(availible_drivers))):
                
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
                if (driver_node, passenger_node) in self.past_times:
                    pickup_time = self.past_times[(driver_node, passenger_node)]
                    print("pickup time matched")
                    self.match_counter += 1
                else:
                    pickup_time = self.map.get_time(driver_node, passenger_node, hour)
                    print("pickup time not matched")
                # self.past_times[(driver_node, passenger_node, hour)] = pickup_time
                end_time = time.time()
                self.get_shortest_path_total_time += (end_time - start_time)
                self.get_shortest_path_total_calls += 1

                if (pickup_time < min_time):
                    min_time = pickup_time
                    min_driver = i
                
                if pickup_time <= 0.1:
                    break

            driver_id = availible_drivers[min_driver][1]
            del availible_drivers[min_driver]
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, pickup_time=min_time, heuristic="manhattan")
        else:
            driver_id = availible_drivers[0][1]
            del availible_drivers[0]
            driver_return_to_road = self.complete_ride(driver_id, passenger_id, heuristic="manhattan")
            
        if driver_return_to_road:
            heapq.heappush(self.drivers_pq, (self.drivers[driver_id]["time"],
                                            driver_id,
                                            self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]))