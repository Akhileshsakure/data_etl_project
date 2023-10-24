import numpy as np
import pandas as pd

mathematica_df = pd.read_csv("C:/Users/akhil/study/python_workspace/data_etl_project/mathematica.csv", index_col="OrderID")
mathematica_df['OrderDate'] = pd.to_datetime(mathematica_df['OrderDate'], errors='coerce')

#Remove duplicates
mathematica_df.drop_duplicates(inplace=True)

#Create TotalCost
mathematica_df['TotalCost'] = mathematica_df['Quantity'] * mathematica_df['UnitPrice']

#Create Month column
mathematica_df['Month'] = mathematica_df['OrderDate'].dt.month
print(mathematica_df)

#Product groupby Quantity
product_sum_df = mathematica_df.groupby(by="ProductID")['Quantity'].sum()
# print(type(product_sum_df))

#UnitPrice average
product_mean_df = mathematica_df.groupby(by="ProductID")['UnitPrice'].mean()

# mathematica_df.to_csv("transformed_data.csv")
# product_sum_df.to_csv("Product_sum.csv")
# product_mean_df.to_csv("Product_mean.csv")