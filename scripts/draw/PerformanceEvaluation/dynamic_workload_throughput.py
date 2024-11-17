import os
import sys
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.font_manager import FontProperties

# 图表样式设置
OPT_FONT_NAME = 'Helvetica'
TICK_FONT_SIZE = 24
LABEL_FONT_SIZE = 28
LEGEND_FONT_SIZE = 30
LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
LEGEND_FP = FontProperties(style='normal', size=LEGEND_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)

# 折线样式和颜色设置
COLOR_MAP = ['#86221F','#005B8E', '#028840']
LINE_STYLES = ['-', '-', '-']
MARKERS = ['s', 'v', '^']
LINE_WIDTH = 3.0
MARKER_SIZE = 10

# project_Dir = os.environ.get("project_Dir", "/default/path/to/project")
project_Dir = os.environ.get("project_Dir", "/Users/curryzjj/hair-loss/Draw/MorphStream")
data_path = os.path.join(project_Dir, "result/data/DynamicWorkload/stats")
FIGURE_FOLDER = os.path.join(project_Dir, "result/figures")

def generate_address(app, schedule, threads, total_events, num_items, ratio_of_deposit, state_access_skewness,
                     ratio_of_overlapped_keys, ratio_of_transaction_aborts, transaction_length, is_cyclic, complexity):
    # 格式化生成拼接后的地址
    address = f"{app}/{schedule}/threads = {threads}/totalEvents = {total_events}/{num_items}_{ratio_of_deposit}_{state_access_skewness}_" \
              f"{ratio_of_overlapped_keys}_{ratio_of_transaction_aborts}_{transaction_length}_{is_cyclic}_{complexity}"
    return address

def extract_phase_throughput(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
        start_index = None
        for i, line in enumerate(lines):
            if line.strip().startswith("phase_id"):  # 定位 phase_id 行
                start_index = i + 1
                break

        if start_index is None:
            raise ValueError("No 'phase_id throughput' section found in the file.")

        throughput_values = []
        for line in lines[start_index:start_index + 13]:  # 只取 13 行
            parts = line.split()
            if len(parts) == 2:
                throughput = float(parts[1])
                throughput_values.append(throughput)

        if len(throughput_values) != 13:
            raise ValueError(f"Expected 13 throughput values, but got {len(throughput_values)}.")

        return throughput_values
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return []

import matplotlib.pyplot as plt
import os

# 假设 COLOR_MAP, MARKERS, LINE_WIDTH, MARKER_SIZE, LABEL_FP, LEGEND_FP, FIGURE_FOLDER 已定义

def DrawFigure(x_values, y_values, legend_labels, x_label, y_label, filename):
    fig, ax = plt.subplots(figsize=(12, 5))

    # 绘制每个曲线
    for i in range(len(y_values)):
        ax.plot(x_values, y_values[i], label=legend_labels[i],
                color=COLOR_MAP[i], marker=MARKERS[i], linewidth=LINE_WIDTH,
                markersize=MARKER_SIZE, markerfacecolor=COLOR_MAP[i])

    # 设置自定义的横坐标标签
    # 区间的中点，1-3的中点是2，4-6的中点是5，以此类推
    ax.set_xticks([2, 5, 8, 11])  # 设置横坐标的中间位置
    ax.set_xticklabels(["1-3: Phase 1", "4-6: Phase 2", "7-9: Phase 3", "10-12: Phase 4"])

    # 标签设置
    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)
    plt.grid(axis='y', color='gray', linestyle='--', linewidth=0.5)

    # 图例设置
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles[::1], labels[::1],  # 反转图例顺序（如果需要）
        loc='upper center',  # 图例位置：顶部居中
        prop=LEGEND_FP,  # 图例字体属性
        ncol=3,  # 图例列数
        bbox_to_anchor=(0.5, 1.1),  # 控制图例位置
        handletextpad=0.1,  # 图例图形与文字的间距
        borderaxespad=0.0,  # 图例与图形的间距
        handlelength=1.8,  # 图例句柄长度
        labelspacing=0.3,  # 图例条目间距
        columnspacing=0.3  # 图例列间距
    )

    # 创建输出文件夹
    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)

    # 保存为 PDF 并显示图表
    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight', format='pdf')


if __name__ == "__main__":
    app = "StreamLedger"
    schedule = "daily_schedule"  # 这里添加了 schedule
    threads = 24
    total_events = 3194880
    num_items = 491520
    ratio_of_deposit = 100
    state_access_skewness = 0
    ratio_of_overlapped_keys = 10
    ratio_of_transaction_aborts = 0
    transaction_length = 1
    is_cyclic = "true"
    complexity = 8000

    variants = ["OG_BFS_A", "TStream", "PAT"]
    throughput_results = []
    for variant in variants:
        if variant == "OG_BFS_A":
            is_cyclic = "true"
        else:
            is_cyclic = "false"
        target_dir = generate_address(app, variant, threads, total_events, num_items, ratio_of_deposit,
                                      state_access_skewness,
                                      ratio_of_overlapped_keys, ratio_of_transaction_aborts, transaction_length,
                                      is_cyclic, complexity)
        full_path = os.path.join(data_path, target_dir)
        print(full_path)
        throughputs = extract_phase_throughput(full_path)
        if throughputs:
            throughput_results.append(throughputs)

    legend_labels = ["MorphStream", "T-Stream", "S-Store"]
    x_values = list(range(1, 14))  # 横坐标从 1 到 13

    # 目标文件名

    print(throughput_results)

    # 绘制折线图
    DrawFigure(x_values, throughput_results, legend_labels, '', 'Throughput (K/sec)', "Figure11_a")
