import pandas as pd
import matplotlib.pyplot as plt
from adjustText import adjust_text
import sys


def real_mode(m):
    if m == "mixed":
        return "Mixed (70% Get, 30% Insert)"
    elif m == "get":
        return "Random Get"
    elif m == "scan":
        return "Sequential Scan"
    return m.capitalize()


name = sys.argv[1]
prefix = name.split(".")[0]

# read benchmark data
# keep compatibility with older csv files that used elapsed/elasped
# and normalize to elapsed_us

df = pd.read_csv(f"./{name}")
if "elapsed_us" not in df.columns:
    if "elapsed" in df.columns:
        df = df.rename(columns={"elapsed": "elapsed_us"})
    elif "elasped" in df.columns:
        df = df.rename(columns={"elasped": "elapsed_us"})

# group by mode
modes = df["mode"].unique()

for mode in modes:
    plt.figure(figsize=(16, 9))
    subset = df[df["mode"] == mode]

    # group by key/value size
    key_value_combinations = subset.groupby(["key_size", "value_size"])

    texts = []
    for (key_size, value_size), group in key_value_combinations:
        label = f"key={key_size}B, val={value_size}B"
        x = group["threads"]
        y = group["ops"]

        # draw line
        line, = plt.plot(x, y, marker="o", label=label)

        # add labels
        for xi, yi, ops in zip(x, y, group["ops"]):
            texts.append(
                plt.text(xi, yi, f"{int(ops)}", color=line.get_color(), fontsize=12)
            )

    adjust_text(texts, arrowprops=dict(arrowstyle="->", color="gray"))

    plt.title(f"{prefix.upper()}: {real_mode(mode)}", fontsize=16)
    plt.xlabel("Threads", fontsize=14)
    plt.ylabel("OPS", fontsize=14)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{prefix}_{mode}.png")
    plt.close()
