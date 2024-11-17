import os

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.font_manager import FontProperties

# 图表样式设置
OPT_FONT_NAME = 'Helvetica'
TICK_FONT_SIZE = 20
LABEL_FONT_SIZE = 24
LEGEND_FONT_SIZE = 20
LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
LEGEND_FP = FontProperties(style='normal', size=LEGEND_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)

COLOR_MAP = ['#FF6300', '#005F5C']  # 橘色和深绿色
LINE_STYLES = ['-', '--', '-.']
MARKERS = ['s', 'v', '^']  # 小方块, 小倒三角, 小正三角
LINE_WIDTH = 2.0
MARKER_SIZE = 8

# project_Dir = os.environ.get("project_Dir", "/default/path/to/project")
project_Dir = os.environ.get("project_Dir", "/Users/curryzjj/hair-loss/Draw/MorphStream")
data_path = os.path.join(project_Dir, "result/data/SchedulingGranularities/stats")
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
            import re
            match = re.search(r"Throughput:\s*([0-9\.]+)", line.strip())
            if match:
                throughput = float(match.group(1))  # 提取并转换为浮动数值
                break  # 找到第一个 Throughput 后停止读取
    return throughput

def plot_comparison_bar_chart(x_values, y_values_1, y_values_2, labels, xlabel, ylabel, filename):
    # 设置柱状图的宽度
    bar_width = 0.35

    # 设置x轴的位置
    index = np.arange(len(x_values))

    # 创建图形
    fig, ax = plt.subplots(figsize=(10, 6))

    # 绘制柱状图
    bar1 = ax.bar(index - bar_width, y_values_1, bar_width, label=labels[0], color=COLOR_MAP[0])
    bar2 = ax.bar(index, y_values_2, bar_width, label=labels[1], color=COLOR_MAP[1])

    # 设置x轴标签和y轴标签
    ax.set_xlabel(xlabel, fontproperties=LABEL_FP, fontsize=TICK_FONT_SIZE)
    ax.set_ylabel(ylabel, fontproperties=LABEL_FP, fontsize=TICK_FONT_SIZE)

    # 设置x轴刻度
    ax.set_xticks(index)
    ax.set_xticklabels(x_values, fontsize=TICK_FONT_SIZE)

    # 图例
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles[::1], labels[::1],  # 反转图例顺序（如果需要）
        loc='upper center',  # 图例位置：顶部居中
        prop=LEGEND_FP,  # 图例字体属性
        ncol=3,  # 图例列数
        bbox_to_anchor=(0.5, 1.0),  # 控制图例位置
        handletextpad=0.1,  # 图例图形与文字的间距
        borderaxespad=0.0,  # 图例与图形的间距
        handlelength=1.8,  # 图例句柄长度
        labelspacing=0.3,  # 图例条目间距
        columnspacing=0.3  # 图例列间距
    )

    # 显示网格
    ax.grid(axis='y', linestyle='--', alpha=0.7)

    # 如果提供了保存路径，保存图表
    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight', format='pdf')


if __name__ == "__main__":
    app = "GrepSum"
    schedule = "daily_schedule"  # 这里添加了 schedule
    threads = 24
    total_events = 983040
    num_items = 12288
    ratio_of_deposit = 100
    state_access_skewness = 0
    ratio_of_overlapped_keys = 0
    ratio_of_transaction_aborts = 0
    transaction_length = 1
    is_cyclic = "false"
    complexity = 0

    variants = [122880, 245760, 491520, 983040, 1966080]
    throughput_results_1 = []
    for variant in variants:
        target_dir = generate_address(app, "OP_NS_A", threads, variant, num_items, ratio_of_deposit,
                                      state_access_skewness,
                                      ratio_of_overlapped_keys, ratio_of_transaction_aborts, transaction_length,
                                      is_cyclic, complexity)
        full_path = os.path.join(data_path, target_dir)
        print(full_path)
        throughput_result = extract_throughput(full_path)
        throughput_results_1.append(throughput_result)
    print(throughput_results_1)

    throughput_results_2 = []
    for variant in variants:
        target_dir = generate_address(app, "OG_NS_A", threads, variant, num_items, ratio_of_deposit,
                                      state_access_skewness,
                                      ratio_of_overlapped_keys, ratio_of_transaction_aborts, transaction_length,
                                      is_cyclic, complexity)
        full_path = os.path.join(data_path, target_dir)
        print(full_path)
        throughput_results = extract_throughput(full_path)
        throughput_results_2.append(throughput_results)
    print(throughput_results_2)
    x_values = [5120,10240,20480,40960,81920]  # x 轴值

# 示例数据
labels = ["f-schedule", "c-schedule"]  # 图例标签

plot_comparison_bar_chart(x_values, throughput_results_1, throughput_results_2, labels, "Punctuation Interval", "Throughput (k/sec)", "Figure16_b")
