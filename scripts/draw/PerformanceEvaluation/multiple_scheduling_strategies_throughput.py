import os
import re

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

# 图表样式设置
TICK_FONT_SIZE = 24
LABEL_FONT_SIZE = 28
LEGEND_FONT_SIZE = 30
LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)

COLOR_MAP = ['#AC000A', '#400053', '#FEA801', '#035E9C', '#007E28']  # 设置不同的颜色

# project_Dir = os.environ.get("project_Dir", "/default/path/to/project")
project_Dir = os.environ.get("project_Dir", "/Users/curryzjj/hair-loss/Draw/MorphStream")
data_path = os.path.join(project_Dir, "result/data/Multiple/stats")
FIGURE_FOLDER = os.path.join(project_Dir, "result/figures")

def generate_address(app, schedule, threads, total_events, num_items, ratio_of_deposit, state_access_skewness,
                     ratio_of_overlapped_keys, ratio_of_transaction_aborts, transaction_length, is_cyclic, complexity):
    # 格式化生成拼接后的地址
    address = f"{app}/{schedule}/threads = {threads}/totalEvents = {total_events}/{num_items}_{ratio_of_deposit}_{state_access_skewness}_" \
              f"{ratio_of_overlapped_keys}_{ratio_of_transaction_aborts}_{transaction_length}_{is_cyclic}_{complexity}"
    return address

def extract_throughput(file_path):
    throughput = None
    with open(file_path, 'r') as f:
        for line in f:
            # 使用正则表达式查找包含 Throughput 的行并提取数值
            match = re.search(r"Throughput:\s*([0-9\.]+)", line.strip())
            if match:
                throughput = float(match.group(1))  # 提取并转换为浮动数值
                break  # 找到第一个 Throughput 后停止读取
    return throughput

def DrawBarChart(y_values, labels, y_label, filename):
    fig, ax = plt.subplots(figsize=(10, 6))

    # 设置柱状图的宽度和位置
    bar_width = 0.5
    index = np.arange(len(labels))  # 每个柱子的位置

    # 绘制柱状图
    bars = ax.bar(index, y_values, bar_width, color=COLOR_MAP[:len(labels)])

    # 将每个柱子的标签设置在柱子下方
    ax.set_xticks(index)
    ax.set_xticklabels(labels, fontproperties=TICK_FP)

    # 设置y轴标签
    ax.set_ylabel(y_label, fontproperties=LABEL_FP)
    plt.grid(axis='y', linestyle='--', color='gray', linewidth=0.5)

    # 创建输出文件夹
    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)

    # 保存为 PDF 并显示图表
    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight', format='pdf')

if __name__ == "__main__":
    app = "TollProcessing"
    schedule = "daily_schedule"  # 这里添加了 schedule
    threads = 24
    total_events = 983040
    num_items = 495120
    ratio_of_deposit = 5
    state_access_skewness = 20
    ratio_of_overlapped_keys = 10
    ratio_of_transaction_aborts = 0
    transaction_length = 1
    is_cyclic = "false"
    complexity = 10000

    variants = ["Nested", "OG_NS", "OG_DFS_A","TStream", "PAT"]
    throughput_results = []
    for variant in variants:
        target_dir = generate_address(app, variant, threads, total_events, num_items, ratio_of_deposit,
                                      state_access_skewness,
                                      ratio_of_overlapped_keys, ratio_of_transaction_aborts, transaction_length,
                                      is_cyclic, complexity)
        full_path = os.path.join(data_path, target_dir)
        print(full_path)
        throughputs = extract_throughput(full_path)
        if throughputs:
            throughput_results.append(throughputs)

    # 标签和数据
    labels = ["Nested", "Plain-1", "Plain-2", "TStream", "S-Store"]

    # 绘制柱状图
    DrawBarChart(throughput_results, labels, 'Throughput (k/sec)', "Figure12_a")
