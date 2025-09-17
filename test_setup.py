# test_setup.py
import pandas as pd
import numpy as np
from prefect import flow

@flow
def test_flow():
    print("Prefect flow is working!")
    return "Success"

# Test pandas
df = pd.DataFrame({
    'sensor_id': [1, 2, 3],
    'value': [23.5, 24.1, 22.8]
})
print("DataFrame created:")
print(df)

# Test prefect
result = test_flow()
print(f"Flow result: {result}")

print("All tests passed! Environment is ready.")