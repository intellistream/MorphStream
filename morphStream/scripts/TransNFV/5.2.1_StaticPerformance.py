import os
import numpy as np
import matplotlib.pyplot as plt

vnf_indices = np.arange(10)
colors = ['#AE48C8', '#F44040', '#15DB3F', '#E9AA18']
markers = ['o', 's', '^', 'D']
vnfLists = ["FW", "NAT", "L4LB", "TD", "PD", "PNM", "SBC", "SIPS", "SCP", "ATS"]

throughput_opennf = [1848.3, 269.4, 255.2, 275.4, 279.5, 268.8, 267.4, 256.9, 265.8, 276.1]
throughput_s6 = [1811.4, 864.8, 894.9, 314.9, 315.1, 374.9, 934.2, 371.0, 313.4, 923.3]
throughput_chc = [1823.2, 893.4, 888.4, 315.0, 322.3, 463.4, 930.5, 370.2, 320.0, 876.9]
throughput_transnfv = [1832.0, 935.7, 933.5, 334.2, 325.3, 471.0, 909.3, 372.8, 332.0, 949.3]

latency_opennf = [1.65, 8.83, 10.01, 8.32, 8.44, 9.14, 9.16, 9.45, 8.87, 8.34]
latency_s6 = [1.71, 2.91, 2.86, 6.68, 6.01, 5.75, 2.71, 5.95, 6.84, 2.71]
latency_chc = [1.70, 2.87, 2.62, 6.53, 6.70, 17.8, 2.71, 5.84, 6.53, 2.87]
latency_transnfv = [1.67, 2.76, 2.71, 6.09, 6.48, 17.8, 2.79, 5.98, 5.87, 2.56]

throughput_opennf_scaled = [x / 1000 for x in throughput_opennf]
throughput_s6_scaled = [x / 1000 for x in throughput_s6]
throughput_chc_scaled = [x / 1000 for x in throughput_chc]
throughput_transnfv_scaled = [x / 1000 for x in throughput_transnfv]


def plot_vnf_throughput():
    plt.figure(figsize=(7, 4.5))
    plt.plot(vnfLists, throughput_opennf_scaled, marker=markers[1], color=colors[1], label='OpenNF', markersize=8,
            markeredgecolor='black', markeredgewidth=1.5)
    plt.plot(vnfLists, throughput_chc_scaled, marker=markers[2], color=colors[2], label='CHC', markersize=8,
            markeredgecolor='black', markeredgewidth=1.5)
    plt.plot(vnfLists, throughput_s6_scaled, marker=markers[3], color=colors[3], label='S6', markersize=8,
            markeredgecolor='black', markeredgewidth=1.5)
    plt.plot(vnfLists, throughput_transnfv_scaled, marker=markers[0], color=colors[0], label='TransNFV', markersize=8,
            markeredgecolor='black', markeredgewidth=1.5)

    plt.xticks(fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel('Stateful VNFs', fontsize=18)
    plt.ylabel('Throughput (M pkt/sec)', fontsize=18)
    plt.legend(bbox_to_anchor=(0.45, 1.23), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    plt.tight_layout()
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)


    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.2.1_throughput.pdf'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))



def plot_vnf_latency():
    # Create the line chart
    plt.figure(figsize=(7, 4.5))

    # Plot lines with markers

    plt.plot(vnfLists, latency_opennf, marker=markers[1], color=colors[1], label='OpenNF', markersize=8,
            markeredgecolor='black', markeredgewidth=1.5)
    plt.plot(vnfLists, latency_chc, marker=markers[2], color=colors[2], label='CHC', markersize=8,
            markeredgecolor='black', markeredgewidth=1.5)
    plt.plot(vnfLists, latency_s6, marker=markers[3], color=colors[3], label='S6', markersize=8,
            markeredgecolor='black', markeredgewidth=1.5)
    plt.plot(vnfLists, latency_transnfv, marker=markers[0], color=colors[0], label='TransNFV', markersize=8,
            markeredgecolor='black', markeredgewidth=1.5)

    plt.xticks(fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel('Stateful VNFs', fontsize=18)
    plt.ylabel('Latency (us)', fontsize=18)
    plt.legend(bbox_to_anchor=(0.45, 1.23), loc='upper center', ncol=4, fontsize=16, columnspacing=0.5)
    plt.grid(True, axis='y', color='gray', linestyle='--', linewidth=0.5, alpha=0.6)

    plt.tight_layout()
    # plt.subplots_adjust(left=0.12, right=0.98, top=0.97, bottom=0.15)
    plt.subplots_adjust(left=0.12, right=0.98, top=0.85, bottom=0.15)


    script_dir = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV"
    figure_name = f'5.2.1_latency.pdf'
    figure_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figure_dir, exist_ok=True)
    plt.savefig(os.path.join(figure_dir, figure_name))

    local_script_dir = "/home/zhonghao/图片"
    local_figure_dir = os.path.join(local_script_dir, 'Figures')
    os.makedirs(local_figure_dir, exist_ok=True)
    plt.savefig(os.path.join(local_figure_dir, figure_name))



if __name__ == '__main__':
    plot_vnf_throughput()
    plot_vnf_latency()
    print("Done")