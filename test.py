import pandas as pd



fpath = 's3://sagemaker-test-recommendations-46532742/test_dataset.csv'
df = pd.read_csv(fpath)
df.head(50)