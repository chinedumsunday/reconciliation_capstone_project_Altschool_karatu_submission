import json
import pandas as pd 
import numpy as np
import logging 
import os
from pymongo import MongoClient
import dotenv
dotenv.load_dotenv()


logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    filename='logs/clean_transactions.log', 
    filemode='a'
)
os.makedirs('logs', exist_ok=True)


def read_jsonl(file_path):
    data = []
    with open(file_path, 'r') as reader:
        for line in reader:
            data.append(json.loads(line.strip()))
    return data


def getting_relevant_data(data):
    try:
        relevant_datafile = []
        for record in data:
            event_type = record.get('event', {}).get('type')
            if event_type == 'heartbeat':
                continue
            relevant_data = {
                'order_id': record.get('entity', {}).get('order', {}).get('id'),
                'customer_email': record.get('entity', {}).get('customer', {}).get('email'),
                'payment_id': record.get('entity', {}).get('payment', {}).get('id'),
                'amount': record.get('payload', {}).get('Amount'),
                'currency': record.get('entity', {}).get('payload', {}).get('Currency', 'USD'),
                'created_at': record.get('event', {}).get('ts'),
                'flags': record.get('payload', {}).get('flags', []),
                'payment_ref': record.get('entity', {}).get('payment', {}).get('provider_ref'),
                'payment_provider': record.get('entity', {}).get('payment', {}).get('provider'),
                'payment_status': record.get('payload', {}).get('status'),
            }
            relevant_datafile.append(relevant_data)
        return relevant_datafile
    except Exception as e:
        logging.error(f'Error extracting relevant data: {e}')


data = read_jsonl('quickcart_data/raw_data.jsonl')
print(f'Total records read: {len(data)}')
relevant_datafile = getting_relevant_data(data)
df = pd.DataFrame(relevant_datafile)
logging.info(f'Extracted relevant data shape: ({df.shape[0]}, {df.shape[1]})')


def clean_transactions(df):
    if df.order_id.isnull().any():
        logging.warning('Missing order_id found. Dropping rows with missing order_id.')
        df = df.dropna(subset=['order_id'])
    if df.payment_id.isnull().any():
        logging.warning('Missing payment_id found. Dropping rows with missing payment_id.')
        df = df.dropna(subset=['payment_id'])
    return df

df1 = clean_transactions(df)


def is_cent(value):
    if pd.isna(value) or value == '':
        return False
    if '.' in str(value) or '$' in str(value) or 'USD' in str(value):
        return False
    return True

def normalize_amount(df1):
    cents_rep = df1['amount'].astype(str).apply(is_cent)
    logging.info(f'Identified {cents_rep.sum()} records in amount column that appear to be in cents format.')
    
    df1['amount_usd'] = df1['amount'].astype(str)
    df1['amount_usd'] = df1['amount_usd'].str.replace(r'[^\d.]', '', regex=True)
    df1['amount_usd'] = df1['amount_usd'].str.strip()
    df1['amount_usd'] = df1['amount_usd'].replace('', float('nan'))
    df1['amount_usd'] = df1['amount_usd'].astype('float64')
    
    df1.loc[cents_rep, 'amount_usd'] = df1.loc[cents_rep, 'amount_usd'] / 100
    logging.info('Normalized amount column by dividing by 100 for records identified as cents.')
    
    df1 = df1[df1['amount_usd'] > 0]
    logging.info('Dropped invalid rows where amount_usd is less than or equal to zero or NaN.')
    
    return df1


def extract_flags(df1):
    df1['flags'] = df1['flags'].apply(lambda x: ','.join(x) if isinstance(x, list) else x)
    logging.info('Extracted flags from list format to comma-separated string format in flags column.')
    return df1

df1 = normalize_amount(df1)
df1 = extract_flags(df1)


def remove_test_transactions(df1):
    if df1['flags'].str.contains('test', na=False).any() or df1['flags'].str.contains('sandbox', na=False).any():
        logging.warning('Transactions flagged as test found. Removing transactions flagged as test from the dataset.')
        df1 = df1.drop(df1[df1['flags'].str.contains('test', na=False)].index)
        df1 = df1.drop(df1[df1['flags'].str.contains('sandbox', na=False)].index)
    return df1

def remove_null_amounts(df1):
    if df1['amount_usd'].isnull().any():
        logging.warning('Null values found in amount_usd column. Dropping rows with null amounts.')
        df1 = df1.dropna(subset=['amount_usd'])
    return df1

def remove_zero_amounts(df1):
    if (df1['amount_usd'] == 0).any():
        logging.warning('Zero values found in amount_usd column. Dropping rows with zero amounts.')
        df1 = df1[df1['amount_usd'] != 0]
    return df1

def fill_na_flags(df1):
    if df1['flags'].isnull().any():
        logging.warning('Null values found in flags column. Filling null values with empty string.')
        df1['flags'] = df1['flags'].fillna('normal')
    return df1

def drop_duplicates(df1):
    if df1.duplicated().any():
        logging.warning('Duplicate records found in the dataset. Dropping duplicate records.')
        df1 = df1.drop_duplicates()
    return df1 

def drop_failed_pending_payments(df1):
    if df1['payment_status'].isin(['FAILED', 'PENDING']).any():
        logging.warning('Failed or pending payments found in payment_status column. Dropping rows with failed or pending payments.')
        df1 = df1[~df1['payment_status'].isin(['FAILED', 'PENDING'])]
    return df1


df1 = remove_test_transactions(df1)
df1 = remove_null_amounts(df1)
df1 = remove_zero_amounts(df1)
df1 = drop_failed_pending_payments(df1)
df1 = drop_duplicates(df1)
df1 = fill_na_flags(df1)


print(df1.describe())
print(df1.info())
print(df1.isna().sum())
print(df1.duplicated().sum())
print(df1['amount'].apply(lambda x: '-' in str(x)).sum())
print(df1['flags'].value_counts())
print(df1['payment_status'].value_counts())
print(df1.head())


# MongoDB connection (update URI as needed)
mongo_uri = os.getenv('mongo_uri')
client = MongoClient(mongo_uri)

# Database and collection names
db = client['raw_transactions']
collection = db['raw_transactions']

# Insert raw JSON logs directly into MongoDB
if data:
    collection.insert_many(data)
    logging.info(f'Archived {len(data)} raw transactions to MongoDB collection {collection.name}.')
    print(f'Archived {len(data)} raw transactions to MongoDB.')
else:
    logging.warning('No raw data to archive to MongoDB.')


output_dir = 'quickcart_data/cleaned'
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, 'cleaned_transactions.csv')

df1.to_csv(output_path, index=False)
logging.info(f'Cleaned dataset exported to {output_path}')
print(f'Cleaned dataset exported to {output_path}')