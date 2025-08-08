import talib
import numpy as np

# Create dummy data
close_prices = np.random.random(100)

# Calculate a simple indicator (SMA)
sma = talib.SMA(close_prices, timeperiod=10)

print("TA-Lib installed and working. SMA output:")
print(sma)
