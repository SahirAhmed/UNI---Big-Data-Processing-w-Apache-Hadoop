#pip install pandas

import pandas as  pd

df = pd.read_json("scams.json")
df.to_csv("scamseasier.csv")
