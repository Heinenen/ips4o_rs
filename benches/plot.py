import json
import math
from collections import defaultdict
import matplotlib.pyplot as plt

plt.style.use('seaborn-whitegrid')
distributions = ["uniform", "ones", "sorted", "reverse", "almost_sorted", "unsorted_tail", "exponential", "root_dups", "root_center_dups", "p78center_dups"]
fig, ax = plt.subplots()
legend = []

def json_read(filename):
    with open(filename) as file:
        print(file)
        for line in file:
            yield json.loads(line)

def plot_total_time(bench, algo, distribution):
    lists = bench[algo][distribution].items()
    x, y = zip(*lists)
    ax.plot(x, y, label=distribution)
    ax.set_xlabel("number of elements (log2(n))")
    ax.set_ylabel("total time (s)")

def plot_time_per_element(bench, algo, distribution):
    lists = list(bench[algo][distribution].items())
    for i, (x, y) in enumerate(lists):
        count = 1 << int(x)
        lists[i] = (x, y / (math.log(count, 2) * count))
    x, y = zip(*lists)
    ax.plot(x, y, label=distribution)
    ax.set_xlabel("number of elements (log2(n))")
    ax.set_ylabel("time per element (time/n log(n))")

json_objs = json_read("./bench.json")
bench = {}
for j in json_objs:
    if j["reason"] != "benchmark-complete":
        continue
    id = j["id"]
    mean = j["mean"]["estimate"]
    id_fields = id.split("/")
    group = id_fields[0]
    algo = id_fields[1]
    distribution = id_fields[2]
    exp = id_fields[3][2:]
    count = id_fields[4]

    if algo not in bench:
        dist_dict = { exp : mean }
        algo_dict = { distribution : dist_dict }
        bench[algo] = algo_dict
    
    if distribution not in bench[algo]:
        dist_dict = { exp : mean }
        bench[algo][distribution] = dist_dict

    bench[algo][distribution][exp] = mean

for d in distributions:
    plot_time_per_element(bench, "ips4o_rs_par", d)
    # plot_total_time(bench, "ips4o_rs_par", d)
ax.legend()
plt.savefig("target/python_plot.svg")
plt.show()
