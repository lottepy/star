import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

data = pd.read_csv(
	'../data/msci_moving_return.csv',
	index_col=0,
	parse_dates=True
)
# data = data.dropna()

data.boxplot(column=['1yr', '2yr', '5yr', '10yr'])
plt.show()
print(data.describe())