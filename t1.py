
import heapq
from collections import deque

from utils import *
from algorithms import *
import time

from datetime import datetime

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Contains driver states for simulation
start_time = time.time()
t1_matcher = T1_Matcher()
end_time = time.time()
print("Pre-process time:", end_time - start_time)

# Priority queue of availible drivers
availible_drivers = deque()
# List of all unmatched passengers by increasing time
unmatched_passengers = deque([[id, data] for id, data in t1_matcher.passengers.items()])
# Unmatched at current time
curr_unmatched_passengers = deque([unmatched_passengers.popleft()])
# Time of simulation start is the time of the first passenger, since it is sorted by time increasing
curr_time = curr_unmatched_passengers[0][1]["time"]

# Summary statistics
plot = []
passenger_drivers = []
start_time = time.time()

# Begin simulation
while len(unmatched_passengers) > 0 and len(curr_unmatched_passengers) > 0:
    # Check to see if any new drivers have logged on
    # Add all drivers availible at current time to the availible drivers priority queue (sorted by increasing time)
    while t1_matcher.drivers_pq[0][0] <= curr_time:
        data = heapq.heappop(t1_matcher.drivers_pq)
        availible_drivers.append((data[0], data[1], data[2], data[3]))

    # Keep track of number of passengers looking for a ride and the number of availible drivers
    passenger_drivers.append((curr_time, len(curr_unmatched_passengers), len(availible_drivers)))
    
    # Match all availible drivers to customers
    while len(availible_drivers) > 0 and len(curr_unmatched_passengers) > 0:
        # Since curr_unmatched_passengers is sorted increasing by time
        # this will be the longest waiting passenger
        passenger = curr_unmatched_passengers.popleft()
        t1_matcher.match(availible_drivers, passenger[0])
        plot.append((curr_time, t1_matcher.d1, t1_matcher.d2))

    # Set the current time to the next unmatched passenger's log-in time
    curr_unmatched_passengers.append(unmatched_passengers.popleft())

    if len(unmatched_passengers) > 0:
        curr_time = unmatched_passengers[0][1]["time"]

    print(len(unmatched_passengers), len(curr_unmatched_passengers), len(availible_drivers))

    end_time = time.time()
    execution_time = end_time - start_time
    print("T1 total runtime:", execution_time)

# Print a summary of the experiments for D1, D2, D3
t1_matcher.summarize_experiments()

# Plotting
fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(10, 8))

# Summary statistics for desiderata
plot = sorted(plot)
x_times = [t[0] for t in plot]
y_d1 = [t[1] for t in plot]
y_d2 = [t[2] for t in plot]

ax1.plot(x_times, y_d1, label='D1')
ax1.set_ylabel('Cumulative passenger time wasted')
ax2.plot(x_times, y_d2, label='D2')
ax2.set_ylabel('Cumulative driver time wasted')

# Beautify the x-labels
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
ax1.xaxis.set_major_locator(mdates.DayLocator())
ax1.legend()

ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
ax2.xaxis.set_major_locator(mdates.DayLocator())
ax2.legend()

fig.autofmt_xdate()

# Creating the plot
plt.figure(figsize=(10, 6))

# Summary statistics for desiderata
plot = sorted(plot)
x_times = [t[0] for t in passenger_drivers]
y_passengers = [t[1] for t in passenger_drivers]
y_drivers = [t[2] for t in passenger_drivers]

plt.plot(x_times, y_passengers, marker='o', linestyle='-', color='blue', label='Passengers')
plt.plot(x_times, y_drivers, marker='s', linestyle='--', color='red', label='Drivers')

plt.xlabel('Time')
plt.ylabel('# of People')
plt.title("Passenger Demand and Driver Availibility")

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator())
plt.legend()

plt.legend()
plt.grid(True)
plt.tight_layout()

# Show the plot
plt.show()

# 27 seconds for first 200 samples