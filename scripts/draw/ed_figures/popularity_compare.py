import csv
import matplotlib
import matplotlib.pyplot as plt
from collections import defaultdict
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import LinearLocator, LogLocator, MaxNLocator
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Event mapping dictionary
event_map = {
    "oklahoma": "oklahoma_tornado",
    "texa": "west_texas_explosion",
    "alberta": "alberta_floods",
    #"storm": "hurricane_sandy",
    #"flood": "alberta_floods",
    "abflood": "alberta_floods",
    "albertan": "alberta_floods",
    "boston": "boston_bombings",
    #"boom": "boston_bombings",
    #"bomb": "boston_bombings",
    #"terrorist": "boston_bombings",
    #"marathon": "boston_bombings",
    "tornado": "oklahoma_tornado",
    #"explos": "west_texas_explosion",
    #"west": "west_texas_explosion",
    "sandi": "hurricane_sandy",
    "sandy": "hurricane_sandy",
    "hurrican": "hurricane_sandy",
    # Add more word-event mappings as needed
}

# Color mapping dictionary
color_map = {
    "oklahoma_tornado": "red",
    "west_texas_explosion": "blue",
    "alberta_floods": "green",
    "boston_bombings": "orange",
    "hurricane_sandy": "purple",
    # Add more category-color mappings as needed
}

# Marker mapping dictionary
marker_map = {
    "oklahoma_tornado": "o",
    "west_texas_explosion": "s",
    "alberta_floods": "v",
    "boston_bombings": "^",
    "hurricane_sandy": "D",
    # Add more category-marker mappings as needed
}

event_name_map = {
    "oklahoma_tornado": "Oklahoma Tornado",
    "west_texas_explosion": "West Texas Explosion",
    "alberta_floods": "Alberta Floods",
    "boston_bombings": "Boston Bombings",
    "hurricane_sandy": "Hurricane Sandy",
}

OPT_FONT_NAME = 'Helvetica'
TICK_FONT_SIZE = 26
LABEL_FONT_SIZE = 26
LEGEND_FONT_SIZE = 26
LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
LEGEND_FP = FontProperties(style='normal', size=LEGEND_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)
LINE_SIZE = 2
MARKER_SIZE = 6

MARKERS = (['o', 's', 'v', "^", "h", "v", ">", "x", "d", "<", "|", "", "|", "_"])
LABEL_WEIGHT = 'bold'
LINE_WIDTH = 3.0
MARKER_SIZE = 0.0
MARKER_FREQUENCY = 1000

matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True
matplotlib.rcParams['xtick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['ytick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['font.family'] = OPT_FONT_NAME


# Plotting ED groundTruth
ed_groundTruth_file = "/Users/zhonghao/Downloads/ED_Dataset_unique/dataset_origin.csv"
def plot_ground_truth(ax):
    DATA = pd.read_csv(ed_groundTruth_file)
    RECORD_GAP = 400
    unique_events = DATA['event'].unique()
    events_tweets_num = dict(zip(unique_events, [0] * len(unique_events)))
    gaped_dataset = pd.DataFrame()

    last_used_gap_num_record = pd.Series(index=events_tweets_num.keys())
    first_row = True
    for index, row in DATA.iterrows():
        events_tweets_num[row["event"]] += 1
        new_used_tweet_nums = pd.Series(events_tweets_num.values(), index=events_tweets_num.keys())

        if row["timestamp"] % RECORD_GAP == 0:
            if first_row:
                gaped_dataset = pd.DataFrame(events_tweets_num, index=[0])
                last_used_gap_num_record = pd.Series(events_tweets_num.values(), index=events_tweets_num.keys())
                first_row = False
            else:
                gaped_dataset.loc[len(gaped_dataset)] = new_used_tweet_nums - last_used_gap_num_record
                last_used_gap_num_record = new_used_tweet_nums

    x = gaped_dataset.index
    y2 = gaped_dataset["2012_Sandy_Hurricane"]
    y3 = gaped_dataset["2013_Alberta_Floods"]
    y4 = gaped_dataset["2013_Boston_Bombings"]
    y5 = gaped_dataset["2013_Oklahoma_Tornado"]
    y7 = gaped_dataset["2013_West_Texas_Explosion"]

    # Filter the data based on x values
    mask = x <= 78
    x = x[mask]
    y2 = y2[mask]
    y3 = y3[mask]
    y4 = y4[mask]
    y5 = y5[mask]
    y7 = y7[mask]

    # Plot the vertical lines at the position of the maximum y-values
    ax.axvline(x=x[y2.idxmax()], color=color_map.get("hurricane_sandy"), linestyle='--', linewidth=2)
    ax.axvline(x=x[y3.idxmax()], color=color_map.get("alberta_floods"), linestyle='--', linewidth=2)
    ax.axvline(x=x[y4.idxmax()], color=color_map.get("boston_bombings"), linestyle='--', linewidth=2)
    ax.axvline(x=x[y5.idxmax()], color=color_map.get("oklahoma_tornado"), linestyle='--', linewidth=2)
    ax.axvline(x=x[y7.idxmax()], color=color_map.get("west_texas_explosion"), linestyle='--', linewidth=2)

    # Normalize and round up x-axis labels
    normalized_x = np.ceil(x / 10)
    ax.set_xticks(np.arange(0, 9) * 10)
    ax.set_xticklabels(np.arange(0, 9))
    marker_size = 7

    ax.plot(x, y2, label="Sandy Hurricane", linewidth=2, color=color_map.get("hurricane_sandy"), marker=marker_map.get("hurricane_sandy"), markersize=marker_size)
    ax.plot(x, y3, label="Alberta Floods", linewidth=2, color=color_map.get("alberta_floods"), marker=marker_map.get("alberta_floods"), markersize=marker_size)
    ax.plot(x, y4, label="Boston Bombings", linewidth=2, color=color_map.get("boston_bombings"), marker=marker_map.get("boston_bombings"), markersize=marker_size)
    ax.plot(x, y5, label="Oklahoma Tornado", linewidth=2, color=color_map.get("oklahoma_tornado"), marker=marker_map.get("oklahoma_tornado"), markersize=marker_size)
    ax.plot(x, y7, label="West Texas Explosion", linewidth=2, color=color_map.get("west_texas_explosion"), marker=marker_map.get("west_texas_explosion"), markersize=marker_size)

    ax.set_ylabel("Expected Popularity", fontproperties=LABEL_FP)

    legend = ax.legend(prop=LEGEND_FP,
                    loc='upper center',
                    ncol=3,
                    bbox_to_anchor=(0.46, 1.38),
                    shadow=False,
                    columnspacing=0.7,
                    frameon=True,
                    borderaxespad=0.0,
                    handlelength=1.5,
                    handletextpad=0.1,
                    labelspacing=0.2)
    legend.get_frame().set_linewidth(2)
    legend.get_frame().set_edgecolor('black')


# Plotting ED results
index = 4
ed_results_file = "/Users/zhonghao/Downloads/ED_Dataset_unique/filtered_events_%d.csv" % index
output_file_path = "/Users/zhonghao/Downloads/ED_Dataset_unique/popularity_%d.pdf" % index
total_events = 32000
window_size = 400
gap = 2
diff = "0-001"
simi = "0-1"

def which_event(words, ts):
    if "oklahoma" in words:
        return "oklahoma_tornado"
    elif "texa" in words:
        return "west_texas_explosion"
    elif "alberta" in words:
        return "alberta_floods"
    elif "abflood" in words:
        return "alberta_floods"
    elif "boston" in words:
        return "boston_bombings"
    elif "tornado" in words:
        return "oklahoma_tornado"
    elif "sandi" in words or "sandy" in words:
        return "hurricane_sandy"
    elif "hurrican" in words:
        return "hurricane_sandy"
    # elif "boston" in words or "boom" in words:
    #     return "boston_bombings"
    # elif "boston" in words or "terrorist" in words:
    #     return "boston_bombings"
    
    
def plot_ed_results(ax):
    # Initialize a defaultdict to store the cumulative rates for each category
    cumulative_rates = defaultdict(list)
    # Read the CSV file and perform event mapping
    with open(ed_results_file, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            words = row[2].lower()
            # ts = float(row[1])
            events = []
            # if which_event(words,ts) != None:
            #     events.append(which_event(words,ts))
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
                region = int(timestamp // window_size)
                # Add the rate to the cumulative rates for the corresponding category and region
                cumulative_rates[category, region].append(rate)

    # Calculate the sum of rates for each category and region
    sum_rates = {}
    for (category, region), rates in cumulative_rates.items():
        event_list = sum_rates.setdefault(category, [0] * 80)
        event_list[region] = sum(rates)
    # print(sum_rates)

    # Plot the cumulative rates for each category with assigned colors and markers
    for category, rates in sum_rates.items():
        color = color_map.get(category, 'black')  # Default color is black if category-color mapping is not specified
        marker = marker_map.get(category, 'o')  # Default marker is 'o' if category-marker mapping is not specified
        event_name = event_name_map.get(category)
        ax.plot(range(len(rates)), rates, label=event_name, linewidth=2, color=color, marker=marker, markersize=7)

    # Find the maximum y-value for each line
    max_rates = {category: max(rates) for category, rates in sum_rates.items()}

    # Plot the vertical lines at the position of the maximum y-values
    for category, rates in sum_rates.items():
        ax.axvline(x=rates.index(max_rates[category]), color=color_map.get(category), linestyle='--', linewidth=2)

    # Normalize and round up x-axis labels
    normalized_ticks = np.ceil(ax.get_xticks() / 10)
    ax.set_xticks(np.arange(0, 9) * 10)
    ax.set_xticklabels(np.arange(0, 9))

    # Set plot labels and title
    ax.set_xlabel('Time (sec)', fontproperties=LABEL_FP)
    ax.set_ylabel('Detected Popularity', fontproperties=LABEL_FP)
    # ax.legend()


# Create a figure and axes for the subplots
fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(13, 9), sharex=True)

# plotting groundtruth events
plot_ground_truth(axes[0])

# plotting deteted events
window_size = 400
plot_ed_results(axes[1])
# axes[0].set_xlim(axes[1].get_xlim())

# Adjust spacing between subplots
# fig.subplots_adjust(top=0.85, bottom=0.13, hspace=0.1)
fig.subplots_adjust(left=0.1, right=0.97, bottom=0.1, top=0.85, hspace=0.1)

# Save the figure
plt.savefig(output_file_path)
plt.savefig("/Users/zhonghao/Downloads/ED_Performance.pdf")

# Show the figure
plt.show()
