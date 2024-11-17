from fileinput import filename

import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import os

# 图表样式设置
OPT_FONT_NAME = 'Helvetica'
TICK_FONT_SIZE = 20
LABEL_FONT_SIZE = 24
LEGEND_FONT_SIZE = 20
LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
LEGEND_FP = FontProperties(style='normal', size=LEGEND_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)

COLOR_MAP = ['#86221F','#005B8E', '#028840']
LINE_STYLES = ['-', '-', '-']
MARKERS = ['s', 'v', '^']
LINE_WIDTH = 2.0
MARKER_SIZE = 8

# project_Dir = os.environ.get("project_Dir", "/default/path/to/project")
project_Dir = os.environ.get("project_Dir", "/Users/curryzjj/hair-loss/Draw/MorphStream")
data_path = os.path.join(project_Dir, "result/data/MulticoreScalability/stats")
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

def draw_line_chart(x_values, y_values_list, labels, xlabel, ylabel, filename):
    # 创建图形
    fig, ax = plt.subplots(figsize=(10, 5))

    # 绘制每条线
    for i, y_values in enumerate(y_values_list):
        ax.plot(x_values, y_values, label=labels[i],
                color=COLOR_MAP[i], linestyle=LINE_STYLES[i],
                marker=MARKERS[i], linewidth=LINE_WIDTH, markersize=MARKER_SIZE)

    # 添加标签
    plt.xlabel(xlabel, fontsize=14)
    plt.ylabel(ylabel, fontsize=14)

    # 添加网格
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # 添加图例
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

    # 保存为 PDF
    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight', format='pdf')

    # 显示图形
    plt.tight_layout()
    plt.show()

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
    y_data = []
    for variant in variants:
        throughput = []
        if variant == "OG_BFS_A":
            is_cyclic = "true"
        else:
            is_cyclic = "false"
        for threads in [1, 4, 8, 12, 16, 20, 24]:
            target_dir = generate_address(app, variant, threads, total_events, num_items, ratio_of_deposit,
                                          state_access_skewness,
                                          ratio_of_overlapped_keys, ratio_of_transaction_aborts, transaction_length,
                                          is_cyclic, complexity)
            full_path = os.path.join(data_path, target_dir)
            throughput_result = extract_throughput(full_path)
            throughput.append(throughput_result)
        y_data.append(throughput)
    x_data = [1, 4, 8, 12, 16, 20, 24]
    labels = ["MorphStream", "TStream", "S-Store"]
    xlabel = "Number of core"
    ylabel = "Throughput (k/sec)"
    filename = "Figure18_b"
    draw_line_chart(x_data, y_data, labels, xlabel, ylabel, filename)
