import pandas as pd
import matplotlib.pyplot as plt
from adjustText import adjust_text

def real_mode(m):
    if m == "mixed":
        return "mixed (70% read, 30% write)"
    return m

# 读取数据
df = pd.read_csv("./x.csv")

# 按 mode 分组
modes = df["mode"].unique()


for mode in modes:
    plt.figure(figsize=(16, 9))
    subset = df[df["mode"] == mode]

    # 按 key_size/value_size 分组
    key_value_combinations = subset.groupby(["key_size", "value_size"])

    texts = []
    for (key_size, value_size), group in key_value_combinations:
        label = f"key={key_size}B, val={value_size}B"
        x = group["threads"]
        y = group["ops"]

        # 绘制折线
        line, = plt.plot(x, y, marker="o", label=label)

        # 添加文本标签
        for xi, yi, ops in zip(x, y, group["ops"]):
            texts.append(
                plt.text(xi, yi, f"{int(ops)}", color=line.get_color(), fontsize=12)
            )

    # 自动调整文本位置
    adjust_text(texts, arrowprops=dict(arrowstyle="->", color='gray'))

    # 设置图表样式
    plt.title(f"Performance: {real_mode(mode).upper()}", fontsize=16)
    plt.xlabel("Threads", fontsize=14)
    plt.ylabel("OPS", fontsize=14)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{mode}.png")
    plt.close()