import matplotlib.pyplot as plt
import numpy as np

plt.style.use("_mpl-gallery-nogrid")


# make data
labels = "One", "Two", "Three", "Four"
data = [1, 2, 3, 4]
explode = (0, 0.1, 0, 0)
colors = plt.get_cmap('Greens')(np.linspace(0.2, 0.7, len(data)))

# plot
fig, ax = plt.subplots()
ax.pie(
    data,
    # explode=explode,
    labels = labels,
    colors=colors,
    autopct="%1.1f%%",
    startangle=90,
    radius=3,
    counterclock=False,
    wedgeprops={"linewidth": 1, "edgecolor": "white"},
    center=(4, 4),
    frame=True,
)

ax.set(xlim=(0, 8), xticks=np.arange(1, 8), ylim=(0, 8), yticks=np.arange(1, 8))

plt.show()
