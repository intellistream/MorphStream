import csv
import matplotlib.pyplot as plt
from collections import defaultdict

# Event mapping dictionary
event_map = {
    "sandi": "hurricane_sandy",
    "sandy": "hurricane_sandy",
    "hurrican": "hurricane_sandy",
    "storm": "hurricane_sandy",
    "alberta": "alberta_floods",
    "flood": "alberta_floods",
    "abflood": "alberta_floods",
    "albertan": "alberta_floods",
    "boston": "boston_bombings",
    "boom": "boston_bombings",
    "bomb": "boston_bombings",
    "terrorist": "boston_bombings",
    "marathon": "boston_bombings",
    "oklahoma": "oklahoma_tornado",
    "tornado": "oklahoma_tornado",
    "queensland": "queensland_floods",
    "texa": "west_texas_explosion",
    "explos": "west_texas_explosion",
    "west": "west_texas_explosion",
    # Add more word-event mappings as needed
}

input_file = "/Users/zhonghao/Downloads/40000_Server/40000_5_20_10_0_1_false_0_80_100.events"

# Initialize a defaultdict to store the cumulative rates for each category
cumulative_rates = defaultdict(list)

# Read the CSV file and perform event mapping
with open(input_file, 'r') as file:
    csv_reader = csv.reader(file)
    
    for row in csv_reader:
        words = row[2].lower()
        events = []
        for key in event_map.keys():
            if key in words:
                events.append(event_map.get(key))
                break
        
        row += events  # Append events to the end of the row
        
        if len(row) == 6:
            timestamp = float(row[1])
            rate = float(row[4])
            category = row[5]
            
            # Compute the region based on timestamp
            region = int(timestamp // 400)
            
            # Add the rate to the cumulative rates for the corresponding category and region
            cumulative_rates[category, region].append(rate)

# Calculate the sum of rates for each category and region
sum_rates = {}
for (category, region), rates in cumulative_rates.items():
    sum_rates.setdefault(category, []).append(sum(rates))

# Plot the cumulative rates for each category
for category, rates in sum_rates.items():
    plt.plot(range(len(rates)), rates, label=category)

# Set plot labels and title
plt.xlabel('Window (400 events)')
plt.ylabel('Event Popularity')
plt.title('Change of Event Popularity over Windows')

# Display the legend and show the plot
plt.legend()

plt.savefig("/Users/zhonghao/Downloads/40000_Server/40000_event_cdf_full.pdf")
