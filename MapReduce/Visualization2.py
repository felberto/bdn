import pandas as pd
import matplotlib.pyplot as plt

result = pd.read_csv("outputMapReduce2.csv", sep="\t", lineterminator=",", names=['minute', 'average'])
result['minute'] = result['minute'] - result['minute'].min()
result = result.sort_values('minute')

plt.plot(result['minute'], result['average'])
plt.xlabel('minute')
plt.ylabel('average')

plt.show()