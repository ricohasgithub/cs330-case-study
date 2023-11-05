
import heapq

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
        start_time, driver_id, _, _ = heapq.heappop(availible_drivers)
        # Process driver pick up and drop off
        self.complete_ride(driver_id, passenger_id)
        # Re-queue into priority queue with end time of drop off and end position
        heapq.heappush(self.drivers_pq, (self.drivers[driver_id]["time"],
                                         driver_id,
                                         self.drivers[driver_id]["source_lat"], self.drivers[driver_id]["source_lon"]))