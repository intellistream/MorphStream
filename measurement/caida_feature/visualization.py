import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Load the events data from the provided CSV file
events_file_path = '/Users/kailianjacy/pcapSource/events.csv'
events_df = pd.read_csv(events_file_path)

# Function to round time to nearest slot
def round_time_to_slot(time, slot_size=0.01):
    return np.round(time / slot_size) * slot_size

# Calculate intervals
events_df.sort_values(by='Time', inplace=True)
events_df['Interval'] = events_df.groupby('EventType')['Time'].diff()
# events_df['Interval'] = events_df['Time'].diff()

# Adding a TimeSlot column for grouping
events_df['TimeSlot'] = events_df['Time'].apply(lambda t: round_time_to_slot(t))

# Grouping events by type and time slots
grouped_events = events_df.groupby(['EventType', 'TimeSlot'])

# Calculating statistics for each group
stats_df = grouped_events['Interval'].agg(['max', 'min', 'mean', 'var']).reset_index()

# Calculating the count of events in each time slot for each event type
event_counts = grouped_events.size().reset_index(name='Count')

# Merging the count data with the stats data
merged_stats_df = pd.merge(stats_df, event_counts, on=['EventType', 'TimeSlot'])

# Separate dataframes for each event type with counts
event_types = events_df['EventType'].unique()
event_stats_with_counts = {event: merged_stats_df[merged_stats_df['EventType'] == event] for event in event_types}

# Plotting line graphs with secondary y-axis for event counts
fig, axs = plt.subplots(len(event_types), figsize=(10, 6 * len(event_types)), sharex=True)
for i, event_type in enumerate(event_types):
    ax = axs[i] if len(event_types) > 1 else axs
    event_data = event_stats_with_counts[event_type]

    # Plotting interval statistics
    ax.plot(event_data['TimeSlot'], event_data['mean'], label='Average Interval', color='blue')
    ax.fill_between(event_data['TimeSlot'], event_data['min'], event_data['max'], alpha=0.2, color='grey')
    ax.scatter(event_data['TimeSlot'], event_data['max'], label='Max Interval', color='red', marker='^')
    ax.scatter(event_data['TimeSlot'], event_data['min'], label='Min Interval', color='green', marker='v')
    ax.set_ylabel('Interval (s)')
    ax.legend(loc='upper left')

    # Secondary axis for event counts
    ax2 = ax.twinx()
    ax2.plot(event_data['TimeSlot'], event_data['Count'], label='Event Count', color='orange', marker='o', linestyle='--')
    ax2.set_ylabel('Count')
    ax2.legend(loc='upper right')

    ax.set_title(f'Event Type: {event_type}')
    ax.set_xlabel('Time Slot')

plt.tight_layout()
plt.savefig('/Users/kailianjacy/pcapSource/events.png')
# plt.show()