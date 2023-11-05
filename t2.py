
import heapq
from collections import deque

from utils import *
from algorithms import *
import time

# Contains driver states for simulation
t2_matcher = T2_Matcher()

# Priority queue of availible drivers
availible_drivers = []
# List of all unmatched passengers by increasing time
unmatched_passengers = deque([[id, data] for id, data in t2_matcher.passengers.items()])
# Unmatched at current time
curr_unmatched_passengers = deque([unmatched_passengers.popleft()])
# Time of simulation start is the time of the first passenger, since it is sorted by time increasing
curr_time = curr_unmatched_passengers[0][1]["time"]

# Begin simulation
while len(unmatched_passengers) > 0 and len(curr_unmatched_passengers) > 0:
    
    # Check to see if any new drivers have logged on
    # Add all drivers availible at current time to the availible drivers priority queue (sorted by increasing time)
    while t2_matcher.drivers_pq[0][0] <= curr_time:
        data = heapq.heappop(t2_matcher.drivers_pq)
        availible_drivers.append((data[0], data[1], data[2], data[3]))

    # Match all availible drivers to customers
    while len(availible_drivers) > 0 and len(curr_unmatched_passengers) > 0:
        # Since curr_unmatched_passengers is sorted increasing by time
        # this will be the longest waiting passenger
        passenger = curr_unmatched_passengers.popleft()
        t2_matcher.match(availible_drivers, passenger[0])

    # Set the current time to the next unmatched passenger's log-in time
    curr_unmatched_passengers.append(unmatched_passengers.popleft())

    if len(unmatched_passengers) > 0:
        curr_time = unmatched_passengers[0][1]["time"]

    print(len(unmatched_passengers), len(curr_unmatched_passengers))