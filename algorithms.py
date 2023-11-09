
import heapq
import math

from utils import *
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
        start_time, driver_id, _, _ = heapq.heappop(availible_drivers)
        # Process driver pick up and drop off
        self.complete_ride(driver_id, passenger_id)
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
        self.complete_ride(driver_id, passenger_id)

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
                hour = max(self.drivers[driver_id]["time"].hour, self.passengers[passenger_id]["time"].hour)
                # Calculate driving time for driver to reach passenger
                pickup_time = self.map.get_time(driver_node, passenger_node, hour, 'djikstras')

                if (pickup_time < min_time):
                    min_time = pickup_time
                    min_driver = i

            driver_id = availible_drivers[min_driver][1]
            del availible_drivers[min_driver]
            self.complete_ride(driver_id, passenger_id)
        else:
            driver_id = availible_drivers[0][1]
            del availible_drivers[0]
            self.complete_ride(driver_id, passenger_id)

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

        # Build a quad tree out of all nodes in the map
        min_lat, min_lon, max_lat, max_lon = self.get_map_bounds()
        boundary = Box((min_lat + max_lat) / 2, (min_lon + max_lon) / 2, abs(max_lat - min_lat), abs(max_lon - min_lon))
        self.quad_tree = QuadTree(boundary, 4)
        for node, data in self.map.node_to_latlon.items():
            self.quad_tree.insert(Point(data["lat"], data["lon"], id=node))

    def get_map_bounds(self):
        # Get min, max bounds for lat, lon over all nodes
        min_lat, min_lon, max_lat, max_lon = float("inf"), float("inf"), float("-inf"), float("-inf")
        for node, data in self.map.node_to_latlon.items():
            min_lat = min(min_lat, data["lat"])
            min_lon = min(min_lon, data["lon"])
            max_lat = max(max_lat, data["lat"])
            max_lon = max(max_lon, data["lon"])
        return min_lat, min_lon, max_lat, max_lon
    
    def get_closest_nodes(self, lat, lon):
        query = Point(lat, lon, id=0)
        closest_point, _ = self.quad_tree.find_closest_point(query)
        return closest_point.id
    
    # Get best driver for a given passenger by finding first availible driver
    def match(self, availible_drivers, passenger_id):

        # Get the closest available driver by euclidean distance
        min_time = float("inf")
        min_driver = None

        # Find closest nodes to each of driver and passenger
        passenger_node = self.get_closest_nodes(self.passengers[passenger_id]["source_lat"], self.passengers[passenger_id]["source_lon"])
        
        for i in range(len(availible_drivers)):

            driver = availible_drivers[i]
            driver_id = driver[1]
            driver_node = self.get_closest_nodes(self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]) if not driver_id in self.nearest_nodes.keys() else self.nearest_nodes[driver_id]
            self.nearest_nodes[driver_id] = driver_node

            # Calculate starting drive hour
            hour = max(self.drivers[driver_id]["time"].hour, self.passengers[passenger_id]["time"].hour)
            # Calculate driving time for driver to reach passenger
            pickup_time = self.map.get_time(driver_node, passenger_node, hour)

            if (pickup_time < min_time):
                min_time = pickup_time
                min_driver = i

        driver_id = availible_drivers[min_driver][1]
        del availible_drivers[min_driver]
        self.complete_ride(driver_id, passenger_id)

        heapq.heappush(self.drivers_pq, (self.drivers[driver_id]["time"],
                                         driver_id,
                                         self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]))