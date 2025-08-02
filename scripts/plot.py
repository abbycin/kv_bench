import pandas as pd
import matplotlib.pyplot as plt

# 读取数据
df = pd.read_csv("./x.csv")

# group by mode
modes = df["mode"].unique()

for mode in modes:
    plt.figure(figsize=(12, 6))
    subset = df[df["mode"] == mode]

    # group by key_size/value_size
    key_value_combinations = subset.groupby(["key_size", "value_size"])

    for (key_size, value_size), group in key_value_combinations:
        label = f"key={key_size}B, val={value_size}B"
        plt.plot(group["threads"], group["ops"], marker="o", label=label)

    plt.title(f"Performance: {mode.upper()}")
    plt.xlabel("Threads")
    plt.ylabel("OPS")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{mode}.png")
    plt.close()
