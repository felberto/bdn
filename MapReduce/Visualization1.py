import matplotlib.pyplot as plt
import pandas as pd

result = pd.read_csv("outputMapReduce1.csv", sep="\t", lineterminator=",", names=['minute', 'throughput'])
result['minute'] = result['minute'] - result['minute'].min()
result = result.sort_values('minute')

plt.plot(result['minute'], result['throughput'])
plt.xlabel('minute')
plt.ylabel('throughput')

plt.show()
