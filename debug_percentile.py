# Debug percentile calculation
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from app.calculation.formulas import EducationalPercentileStrategy

strategy = EducationalPercentileStrategy()

# Test with 1-100 scores
scores = list(range(1, 101))  # 1到100的分数
data = pd.DataFrame({'score': scores})
config = {'percentiles': [25, 50, 75]}

result = strategy.calculate(data, config)

print("Scores length:", len(scores))
print("Test results:")
for key, value in result.items():
    print(f"  {key}: {value}")

# Debug floor calculation
n = len(scores)
for p in [25, 50, 75]:
    rank = int(n * p / 100.0)  # This is the issue - we need floor
    import numpy as np
    floor_rank = int(np.floor(n * p / 100.0))
    print(f"P{p}: regular rank={rank}, floor rank={floor_rank}, scores[{floor_rank}]={scores[floor_rank]}")