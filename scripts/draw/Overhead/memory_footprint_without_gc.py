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
LEGEND_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)

COLOR_MAP = ['#86221F','#005B8E', '#028840']
LINE_STYLES = ['-', '-', '-']
MARKERS = ['s', 'v', '^']
LINE_WIDTH = 2.0
MARKER_SIZE = 8

# project_Dir = os.environ.get("project_Dir", "/default/path/to/project")
project_Dir = os.environ.get("project_Dir", "/Users/curryzjj/hair-loss/Draw/MorphStream")
data_path = os.path.join(project_Dir, "result/data/SystemOverhead/stats")
FIGURE_FOLDER = os.path.join(project_Dir, "result/figures")



def generate_address(app, schedule, threads, total_events, num_items, ratio_of_deposit, state_access_skewness,
                     ratio_of_overlapped_keys, ratio_of_transaction_aborts, transaction_length, is_cyclic, complexity):
    # 格式化生成拼接后的地址
    address = f"{app}/{schedule}/threads = {threads}/totalEvents = {total_events}/{num_items}_{ratio_of_deposit}_{state_access_skewness}_" \
              f"{ratio_of_overlapped_keys}_{ratio_of_transaction_aborts}_{transaction_length}_{is_cyclic}_{complexity}.txt"
    return address

def read_data_from_file(file_path):
    x_values = []
    y_values = []
    with open(file_path, 'r') as file:
        lines = file.readlines()
        for line in lines[1:]:  # 跳过第一行（标题）
            parts = line.strip().split()
            if len(parts) == 2:
                try:
                    x_values.append(float(parts[0]))
                    y_values.append(float(parts[1]))
                except ValueError:
                    continue  # 跳过无法解析的行
    return x_values, y_values


def plot_multi_line_from_files(file_paths, labels, xlabel, ylabel, filename):
    fig, ax = plt.subplots(figsize=(12, 6))

    # 从每个文件读取数据并绘制折线
    for i, file_path in enumerate(file_paths):
        x_values, y_values = read_data_from_file(file_path)
        ax.plot(
            x_values, y_values,
            color=COLOR_MAP[i % len(COLOR_MAP)],
            linestyle=LINE_STYLES[i % len(LINE_STYLES)],
            marker=MARKERS[i % len(MARKERS)],
            linewidth=LINE_WIDTH,
            markersize=MARKER_SIZE,
            label=labels[i]
        )

    # 设置x轴和y轴标签
    ax.set_xlabel(xlabel, fontproperties=LABEL_FP)
    ax.set_ylabel(ylabel, fontproperties=LABEL_FP)

    # 设置刻度字体大小
    ax.tick_params(axis='both', which='major', labelsize=TICK_FONT_SIZE)

    # 显示网格
    ax.grid(axis='both', linestyle='--', alpha=0.7)

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

    # 保存图表
    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)
    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight', format='pdf')

    # 显示图表
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
    file_paths = []
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
        file_paths.append(full_path)
    labels=["MorphStream(w/o GC)", "TStream(w/o GC)", "S-Store(w/o GC)"];
    xlabel = "Elapsed Time (s)"
    ylabel = "Memory Consuming (GB)"
    plot_multi_line_from_files(file_paths, labels, xlabel, ylabel, "Figure13_b")