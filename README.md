# python_sql_postgres_ETL
he provided code outlines an ETL pipeline that extracts transaction data from a MySQL database, processes the data (including sorting, calculating balances, and adding new columns), and then loads the processed data into a PostgreSQL database.

The provided code outlines an ETL pipeline that extracts transaction data from a MySQL database, processes the data (including sorting, calculating balances, and adding new columns), and then loads the processed data into a PostgreSQL database.

Here’s a brief breakdown of the process:

Database Connections:

The code creates connections to both MySQL (source_engine) and PostgreSQL (target_postgres_engine), as well as a MySQL target database (target_mysql_engine), using SQLAlchemy.
Date Calculation:

It calculates the date for "yesterday" and the start of the current year to be used for querying.
SQL Query:

The query fetches transaction data for a specific date (based on {i}), which is dynamically set to 4 days ago. It joins multiple tables, filters the data, and formats various columns, such as merchant codes, transaction details, and currency.
Data Processing:

After loading the data into a Pandas DataFrame, the script processes it by:
Sorting the data by marchand (merchant) and date.
Calculating balance_before and balance_finish for each transaction, considering the previous transaction's balance.
Creating a new eme_name column, which combines merchant information with a country name.
Calculating the tva (tax value added) based on the country.
Data Insertion into PostgreSQL:

After data transformation, an INSERT INTO statement is constructed using the text module from SQLAlchemy.
The data is inserted into the PostgreSQL pm_merchant_settlement table. If a transaction ID already exists, the script performs an ON CONFLICT operation to update the existing record with the new values.
Potential Improvements:
Error Handling: Adding try-except blocks could make the script more robust, especially for the database connections and during the data insertion process.
Chunking Inserts: If the dataset is large, consider chunking the insert operations into batches to optimize performance.
Logging: Introduce logging to track progress and catch any issues during the extraction, transformation, and loading stages.
Example for Error Handling and Chunking:
python
Copier le code
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Function to process and load data
def load_data_to_postgres(df, engine, insert_stmt, chunk_size=1000):
    with engine.begin() as connection:
        # Iterate over chunks of the DataFrame
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start:start+chunk_size]
            for index, row in chunk.iterrows():
                try:
                    connection.execute(insert_stmt, **row.to_dict())
                except Exception as e:
                    logger.error(f"Error inserting row {index}: {e}")
                    continue
            logger.info(f"Inserted chunk starting from row {start}")

# ETL Process
try:
    df = pd.read_sql(query, source_engine)

    # Sort and calculate balances
    df = df.sort_values(by=['marchand', 'date'])
    df['balance_before'] = pd.NA
    df['balance_finish'] = pd.NA

    for name, group in df.groupby('marchand'):
        for idx in range(len(group)):
            if idx == 0:
                df.loc[group.index[idx], 'balance_before'] = (
                    df.loc[group.index[idx], 'transaction_balance']  
                    - df.loc[group.index[idx], 'volume']
                ) 
                df.loc[group.index[idx], 'balance_finish'] = (
                    df.loc[group.index[idx], 'transaction_balance'] 
                    - df.loc[group.index[idx], 'commission_ttc']
                )
            else:
                df.loc[group.index[idx], 'balance_before'] = df.loc[group.index[idx - 1], 'balance_finish']
                df.loc[group.index[idx], 'balance_finish'] = (
                    df.loc[group.index[idx], 'balance_before'] 
                    + df.loc[group.index[idx], 'volume'] 
                    - df.loc[group.index[idx], 'commission_ttc']
                )

    # Eme_name and TVA columns
    df['eme_name'] = df['mno_name'] + ' MONEY ' + df['country'].replace({
            'CI': 'Côte d\'Ivoire',
            'BJ': 'Benin',
            'TG': 'Togo',
            'SN': 'Senegal',
            'BF': 'Burkina Faso',
            'CM': 'Cameroon',
            'CD': 'Republique Democratique de Congo',
            'RDC': 'Republique Democratique de Congo'
        }).fillna('Unknown')

    df['tva'] = np.where(df['country'] == 'CM', 19.25 / 100, 18 / 100)

    # Load to PostgreSQL in chunks
    insert_stmt = text("""
        INSERT INTO pm_merchant_settlement
        (order_id, transaction_id, settlement_name, settlement_date, date, mois, 
        marchand, account_number, currency, country, mno_name, operation_type, 
        volume, fees, transaction_balance, available_balance, taux_marchand, 
        account_type, commission_ttc, reversement, balance_before, balance_finish, 
        eme_name, operator_transaction_id, tva, sub_account_number)
        VALUES
        (:order_id, :transaction_id, :settlement_name, :settlement_date, :date, :mois, 
        :marchand, :account_number, :currency, :country, :mno_name, :operation_type, 
        :volume, :fees, :transaction_balance, :available_balance, :taux_marchand, 
        :account_type, :commission_ttc, :reversement, :balance_before, :balance_finish, 
        :eme_name, :operator_transaction_id, :tva, :sub_account_number)
        ON CONFLICT (transaction_id) DO UPDATE SET
            settlement_name = EXCLUDED.settlement_name,
            settlement_date = EXCLUDED.settlement_date,
            date = EXCLUDED.date,
            mois = EXCLUDED.mois,
            marchand = EXCLUDED.marchand,
            account_number = EXCLUDED.account_number,
            currency = EXCLUDED.currency,
            country = EXCLUDED.country,
            mno_name = EXCLUDED.mno_name,
            operation_type = EXCLUDED.operation_type,
            volume = EXCLUDED.volume,
            fees = EXCLUDED.fees,
            transaction_balance = EXCLUDED.transaction_balance,
            available_balance = EXCLUDED.available_balance,
            taux_marchand = EXCLUDED.taux_marchand,
            account_type = EXCLUDED.account_type,
            commission_ttc = EXCLUDED.commission_ttc,
            reversement = EXCLUDED.reversement,
            balance_before = EXCLUDED.balance_before,
            balance_finish = EXCLUDED.balance_finish,
            eme_name = EXCLUDED.eme_name,
            operator_transaction_id = EXCLUDED.operator_transaction_id,
            tva = EXCLUDED.tva,
            sub_account_number = EXCLUDED.sub_account_number;
    """)

    # Call the function to load data
    load_data_to_postgres(df, target_postgres_engine, insert_stmt)

except Exception as e:
    logger.error(f"ETL process failed: {e}")
