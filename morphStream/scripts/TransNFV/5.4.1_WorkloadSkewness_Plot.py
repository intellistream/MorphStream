import subprocess
import os
import time
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import csv

# Simulated data
key_skewness_values = [0, 0.25, 0.5, 0.75, 1]
strategies = ["Proactive Resolution", "Passive Resolution"]
colors = ['white', 'white']
hatches = ['\\\\\\', '///']
hatch_colors = ['#ed8e11', '#11abed']

# Simulated throughput data (in millions of requests per second)
# Each row corresponds to a different key skewness value.
throughput_data = np.array([
    [80586, 163004],  # Throughput for key skewness 0.1
    [80008, 151647],  # Throughput for key skewness 0.25
    [79857, 154379],  # Throughput for key skewness 0.5
    [80844, 145519],  # Throughput for key skewness 0.75
    [79850, 125594]   # Throughput for key skewness 1
])

# Normalize the throughput by 10^6 (convert to millions of requests per second)
throughput_data = throughput_data / 1e6

# Plotting parameters
bar_width = 0.2
index = np.arange(len(key_skewness_values))

# Plot the data
fig, ax = plt.subplots(figsize=(7,5)) 

for i, strategy in enumerate(strategies):
    ax.bar(index + i * bar_width, throughput_data[:, i],color=colors[i], hatch=hatches[i], edgecolor=hatch_colors[i], width=bar_width, label=strategy)

# Set x-axis labels and positions
ax.set_xticks([r + bar_width for r in range(len(key_skewness_values))])
ax.set_xticklabels(key_skewness_values, fontsize=16)
ax.set_ylabel('Throughput (M req/sec)', fontsize=18, labelpad=12)
ax.set_xlabel('Trojan Detector Workload Variations', fontsize=18, labelpad=12)

ax.tick_params(axis='y', labelsize=14)

# Set labels and title
ax.set_xlabel('Workload Skewness', fontsize=18)
ax.set_ylabel('Throughput (Million req/sec)', fontsize=18)
# ax.set_title('Throughput Comparison of Strategies under Different Key Skewness Values', fontsize=16)


# Create custom legend with hatches
handles = [Patch(facecolor=color, edgecolor=hatchcolor, hatch=hatch, label=label) for color, hatchcolor, hatch, label in zip(colors, hatch_colors, hatches, strategies)]
ax.legend(handles=handles, bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=2, fontsize=16)

plt.tight_layout(rect=[0, 0.03, 1, 0.95])

# plt.show()

# Save the figure in the same directory as the script
script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
figure_dir = os.path.join(script_dir, 'figures')
os.makedirs(figure_dir, exist_ok=True)
plt.savefig(os.path.join(figure_dir, '5.4.1_Workload_Skewness_Throughput.pdf'))  # Save the figure
plt.savefig(os.path.join(figure_dir, '5.4.1_Workload_Skewness_Throughput.png'))  # Save the figure

local_script_dir = "/home/zhonghao/图片"
local_figure_dir = os.path.join(local_script_dir, 'Figures')
os.makedirs(local_figure_dir, exist_ok=True)
plt.savefig(os.path.join(local_figure_dir, '5.4.1_Workload_Skewness_Throughput.pdf'))  # Save the figure