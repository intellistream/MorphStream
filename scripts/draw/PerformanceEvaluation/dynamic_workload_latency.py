import os
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

COLOR_MAP = ['#86221F','#005B8E', '#028840']
LINE_STYLES = ['-', '-', '-']
MARKERS = ['s', 'v', '^']
LINE_WIDTH = 2.0
MARKER_SIZE = 8

# project_Dir = os.environ.get("project_Dir", "/default/path/to/project")
project_Dir = os.environ.get("project_Dir", "/Users/curryzjj/hair-loss/Draw/MorphStream")
data_path = os.path.join(project_Dir, "result/data/DynamicWorkload/stats")
FIGURE_FOLDER = os.path.join(project_Dir, "result/figures")

def generate_address(app, schedule, threads, total_events, num_items, ratio_of_deposit, state_access_skewness,
                     ratio_of_overlapped_keys, ratio_of_transaction_aborts, transaction_length, is_cyclic, complexity):
    # 格式化生成拼接后的地址
    address = f"{app}/{schedule}/threads = {threads}/totalEvents = {total_events}/{num_items}_{ratio_of_deposit}_{state_access_skewness}_" \
              f"{ratio_of_overlapped_keys}_{ratio_of_transaction_aborts}_{transaction_length}_{is_cyclic}_{complexity}.latency"
    return address


import numpy as np


def calculate_percentiles(file_path, percentiles=[0.5, 20, 40, 60, 80, 99]):
    # 读取文件并将延迟数据转换为一个列表，直到遇到 '=======Details======='
    latencies = []
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()

            # 如果遇到 '=======Details=======' 则停止读取
            if line == "=======Details=======":
                break

            try:
                # 试图将行转换为浮动数值并加入列表
                latency = float(line)
                latencies.append(latency)
            except ValueError:
                # 如果不是有效的数字，跳过这行
                continue

    # 检查是否读取到有效数据
    if not latencies:
        raise ValueError("文件中没有有效的延迟数据")

    # 使用 numpy 计算各个百分位数
    results = []
    for p in percentiles:
        # np.percentile 会根据百分位数计算相应的值
        results.append(np.percentile(latencies, p))

    return results


def DrawCDF(x_values_list, y_values_list, legend_labels, x_label, y_label, filename):
    fig, ax = plt.subplots(figsize=(10, 6))

    # 绘制CDF曲线
    for i in range(len(x_values_list)):
        ax.plot(x_values_list[i], y_values_list[i], label=legend_labels[i],
                color=COLOR_MAP[i], linestyle=LINE_STYLES[i],
                marker=MARKERS[i], linewidth=LINE_WIDTH, markersize=MARKER_SIZE)

    # 设置标签
    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)
    plt.grid(axis='both', color='gray', linestyle='--', linewidth=0.5)

    # 图例设置
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles[::1], labels[::1],  # 反转图例顺序（如果需要）
        loc='upper center',  # 图例位置：顶部居中
        prop=LEGEND_FP,  # 图例字体属性
        ncol=3,  # 图例列数
        bbox_to_anchor=(0.5, 1.05 ),  # 控制图例位置
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
    latency_results = []
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
        latency_result = calculate_percentiles(full_path, [0.5, 20, 40, 60, 80, 99])
        print(latency_result)
        latency_results.append(latency_result)

    # 各个系统的延迟数据 (latency in ms) 和累积百分比
    legend_labels = ["MorphStream", "T-Stream", "S-Store"]
    print(latency_results)
    y_values_list = [
        [0.5, 20, 40, 60, 80, 99],  # MorphStream
        [0.5, 20, 40, 60, 80, 99],  # TStream
        [0.5, 20, 40, 60, 80, 99]  # S-Store
    ]

    # 绘制CDF图
    DrawCDF(latency_results, y_values_list, legend_labels, 'Latency (ms)', 'Cumulative Percent (%)',
            "Figure11_b")
