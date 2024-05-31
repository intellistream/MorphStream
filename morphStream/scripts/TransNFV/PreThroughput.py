import matplotlib.pyplot as plt
import numpy as np

# Data for plotting (example throughputs for four systems in four different scenarios)
throughputs = {
    'Lone Operative': [57838, 45102, 26784, 19662],
    'Shared Readers': [42512, 55027, 604, 20571],
    'Shared Writers': [44547, 37393, 29125, 20797],
    'Mutual Interactive': [47053, 165, 542, 22587]
}

systems = ['Partitioning', 'Replication', 'Offloading', 'Preemptive']

# Create a 2x2 subplot
fig, axs = plt.subplots(2, 2, figsize=(10, 8))  # Adjust the figure size as necessary
axs = axs.flatten()  # Flatten the 2D array of axes to 1D for easier indexing

# Loop through scenarios and axes to plot each scenario's data
for i, (scenario, ax) in enumerate(zip(throughputs, axs)):
    y_values = throughputs[scenario]
    x_values = systems
    ax.bar(x_values, y_values, color=['blue', 'green', 'orange', 'yellow'])
    ax.set_title(scenario)
    ax.set_ylabel('Throughput (Request/Sec)')

# Add some space between plots
plt.tight_layout()

# Show the plot
plt.show()
