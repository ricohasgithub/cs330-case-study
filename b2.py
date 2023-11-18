# More equitable number of rides assigned per driver

# We already track the number of rides a passenger does, so we can multiply the distance from driver to passenger by a multiplier based on their ride.

# For example, if there are two drivers and one passenger, and Driver A has done 10 rides and is 10 miles from the passenger,
# while Driver B has done 0 rides but is 11 miles from the passenger, we would multiply Driver A's distance by 1.2 (1 + (.02 * 10 rides)) and Driver B's distance by 1.0.



import heapq
from collections import deque

from utils import *
from bonus_algorithms import *
import time

from datetime import datetime

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Contains driver states for simulation
b2_matcher = B2_Matcher()
# b2_matcher = B2_Default_Matcher()

# Priority queue of availible drivers
availible_drivers = []
# List of all unmatched passengers by increasing time
unmatched_passengers = deque([[id, data] for id, data in b2_matcher.passengers.items()])
# Unmatched at current time
curr_unmatched_passengers = deque([unmatched_passengers.popleft()])
# Time of simulation start is the time of the first passenger, since it is sorted by time increasing
curr_time = curr_unmatched_passengers[0][1]["time"]

# Summary statistics
plot = []
start_time = time.time()

# Begin simulation
while len(unmatched_passengers) > 0 and len(curr_unmatched_passengers) > 0:
    # Check to see if any new drivers have logged on
    # Add all drivers availible at current time to the availible drivers priority queue (sorted by increasing time)
    while b2_matcher.drivers_pq[0][0] <= curr_time:
        data = heapq.heappop(b2_matcher.drivers_pq)
        availible_drivers.append((data[0], data[1], data[2], data[3]))

    # Match all availible drivers to customers
    while len(availible_drivers) > 0 and len(curr_unmatched_passengers) > 0:
        # Since curr_unmatched_passengers is sorted increasing by time
        # this will be the longest waiting passenger
        passenger = curr_unmatched_passengers.popleft()
        b2_matcher.match(availible_drivers, passenger[0])
        plot.append((curr_time, b2_matcher.d1, b2_matcher.d2))

    # Set the current time to the next unmatched passenger's log-in time
    curr_unmatched_passengers.append(unmatched_passengers.popleft())

    if len(unmatched_passengers) > 0:
        curr_time = unmatched_passengers[0][1]["time"]

    print(len(unmatched_passengers), len(curr_unmatched_passengers), len(availible_drivers))
    end_time = time.time()
    execution_time = end_time - start_time
    print("B2 total runtime:", execution_time)
    print("Total D1:", b2_matcher.d1)
    print("Total D2:", b2_matcher.d2)


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

# Labeling and legend
plt.suptitle('Line Plots with Datetime on X-axis')

# Show the plot
plt.show()



# Plot of Driver Equity

# Counting the number of keys in each range
count_0_5 = len([key for key, value in b2_matcher.numDriverRides.items() if 0 <= value <= 5])
count_5_15 = len([key for key, value in b2_matcher.numDriverRides.items() if 5 < value <= 15])
count_15_plus = len([key for key, value in b2_matcher.numDriverRides.items() if value > 15])

# Data for plotting
ranges = ['0-5', '6-15', '16+']
counts = [count_0_5, count_5_15, count_15_plus]

# Plotting
plt.bar(ranges, counts)
plt.xlabel('Rides')
plt.ylabel('Number of Drivers')
plt.title('Equality of Rides Assigned to Drivers')
plt.show()