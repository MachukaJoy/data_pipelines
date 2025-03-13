#!/usr/bin/env python
# coding: utf-8

# # Libraries

# In[1]:


import pandas as pd
import io
import requests
import zipfile
import os


# # Extract

# In[2]:


response = requests.get('https://bit.ly/416WE1X')
response.raise_for_status() # raise an error for bad status codes

with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
    zip_file.extractall(os.getcwd())


# In[3]:


def extract():

    # Read datasets
    dataset1 = pd.read_csv('dataset1.csv')
    dataset2 = pd.read_csv('dataset2.csv')
    dataset3 = pd.read_csv('dataset3.csv')

    # Calculate missing value percentages
    missing1 = dataset1.isnull().mean() * 100
    missing2 = dataset2.isnull().mean() * 100
    missing3 = dataset3.isnull().mean() * 100

    #return data types
    dt_type1 = dataset1.info()
    dt_type2 = dataset2.info()
    dt_type3 = dataset3.info()


    return (dataset1, missing1, dt_type1), (dataset2, missing2,dt_type2), (dataset3, missing3, dt_type3)


# In[16]:


(dataset1, missing1, dt_type1), (dataset2, missing2,dt_type2), (dataset3, missing3, dt_type3) = extract()


# We see promo code in dataset1 has 20% missing values so we will replace the missing values with No Promo just incase one would want to analyze customer purchase behaviour.

# In[52]:


def transform():

    #copy datasets for transformation
    df1 = dataset1.copy()
    df2 = dataset2.copy()
    df3 = dataset3.copy()

    # replace missing values in promo_code column with No Promo
    # df1.fillna({'promo_code': 'No Promo'}, inplace = True)
    df1['promo_code']= df1['promo_code'].fillna('No Promo')
    

    # change date column datatype to date
    df1['date_of_purchase'] = pd.to_datetime(df1['date_of_purchase'], errors='coerce')
    df2['date_of_payment'] = pd.to_datetime(df2['date_of_payment'], errors='coerce')
    df3['date_of_refund'] = pd.to_datetime(df3['date_of_refund'], errors='coerce')

    #merge datasets
    df1.rename(columns={'country_of_purchase': 'country'}, inplace=True) # first rename country column because they are the same accross
    df2.rename(columns={'country_of_payment': 'country'}, inplace=True) # first rename country column because they are the same accross
    df3.rename(columns={'country_of_refund': 'country'}, inplace=True) # first rename country column because they are the same accross

    # now merge df1 and df2 both on the customer_id and country column and payment_method
    df1and2 = df1.merge(df2,on=['customer_id','country','payment_method'], how ='left') 

    # now merge df1and2 to df3 on the customer_id and country_of_purchase/payment/refund column
    merged_df = df1and2.merge(df3, on = ['customer_id','country'], how='left')

    return merged_df


# In[61]:


merged_df = transform()


# In[66]:


def load(data, filename='final_output.csv'):
    data.to_csv(filename, index = False)


# In[74]:


def data_pipeline():
    extract()
    transform()
    load(merged_df)


# In[76]:


data_pipeline()


# In[ ]:


import schedule
import time

def run_pipeline():
    print("Running data pipeline...")
    data_pipeline()
    print("✅ Pipeline completed!")

# Schedule to run every Friday at 8 AM
schedule.every().friday.at("08:00").do(run_pipeline)

print("Scheduler started. Waiting for Friday...")

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute

