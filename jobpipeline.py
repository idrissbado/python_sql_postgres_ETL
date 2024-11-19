import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from sqlalchemy import text
import numpy as np

# Database connection details
source_db_params = {
    'host': '',
    'port': '3306',
    'user': '',
    'password': '',
    'database': 'money_transferdb'
}

target_db_params_postgres = {
    'host': '',
    'port': '5432',
    'user': 'postgres',
    'password': 'geschool2021',
    'database': 'datawarehouse'
}

target_db_params_mysql = {
    'host': '',
    'port': '3306',
    'user': 'bscv3',
    'password': '',
    'database': 'bsc_v3'
}

# Create connection engines for source (MySQL) and target (PostgreSQL, MySQL) databases
source_engine = create_engine(f"mysql+mysqlconnector://{source_db_params['user']}:{source_db_params['password']}@{source_db_params['host']}:{source_db_params['port']}/{source_db_params['database']}")
target_postgres_engine = create_engine(f"postgresql://{target_db_params_postgres['user']}:{target_db_params_postgres['password']}@{target_db_params_postgres['host']}:{target_db_params_postgres['port']}/{target_db_params_postgres['database']}")
target_mysql_engine = create_engine(f"mysql+pymysql://{target_db_params_mysql['user']}:{target_db_params_mysql['password']}@{target_db_params_mysql['host']}:{target_db_params_mysql['port']}/{target_db_params_mysql['database']}")

# Get yesterday's date
yesterday = datetime.now() - timedelta(days=1)

# Get the first day of the current year
start_of_year = datetime(datetime.now().year, 1, 1)

# Calculate the number of days between yesterday and January 1st
days_back = (yesterday - start_of_year).days

# SQL query template with placeholders for the day offset
query_template = """
    SELECT 
    a.order_id AS order_id,
    a.id AS transaction_id,
    CONCAT('Bizao', '_', UPPER(t.service_code), '_', 'daily_', UPPER(t.merchant_reference), '_', DATE(a.date_time)) AS settlement_name,
    a.date_time AS date,
    m.operator_transaction as operator_transaction_id  ,
    DATE(a.date_time) AS settlement_date,
    MONTH(a.date_time) AS mois,
    UPPER(LEFT(a.account_number, 
        CASE 
            WHEN a.account_number LIKE '%_XOF' THEN POSITION('_XOF' IN a.account_number) - 1
            WHEN a.account_number LIKE '%_XAF' THEN POSITION('_XAF' IN a.account_number) - 1
            WHEN a.account_number LIKE '%_USD' THEN POSITION('_USD' IN a.account_number) - 1
            WHEN a.account_number LIKE '%_CDF' THEN POSITION('_CDF' IN a.account_number) - 1
            ELSE LENGTH(a.account_number)
        END
    )) AS marchand,
    a.account_number,
    SUBSTRING_INDEX(a.account_number, '_', -1) AS currency, -- Improved currency extraction
   UPPER( CASE 
        WHEN UPPER(t.destination) = 'CYBER_SOURCE_GATEWAY' THEN 'CI'
        WHEN UPPER(t.destination) = 'ORA_BANK_GATEWAY' THEN 'SN'
        WHEN UPPER(t.destination) = 'FLEXPAY_GATEWAY' THEN 'RDC'
        ELSE t.destination
    END ) AS country,
    UPPER(CASE 
        WHEN UPPER(t.operator) = 'CYBERSOURCE_CI' THEN 'ECOBANK'
        WHEN UPPER(t.operator) = 'ORABANK_SN' THEN 'ORABANK'
        WHEN UPPER(t.operator) = 'FLEXPAY_RDC' THEN 'FLEXPAY'
        ELSE t.operator
    END ) AS mno_name,
    t.service_code AS account_type,
    a.transaction_type AS operation_type,
    a.transaction_amount AS volume,
    COALESCE(a.fees_amount, 0) AS fees,
    a.transaction_balance AS transaction_balance,
    a.settlement_amount as settlement_amount,
    a.settlement_balance  as settlement_balance,
    a.available_balance AS available_balance ,
    COALESCE(a.fees_amount, 1) / NULLIF(a.transaction_amount, 0) AS taux_marchand,
    UPPER(a.sub_account_number) AS sub_account_number ,
    COALESCE(a.fees_amount, 0) + COALESCE(a.vat_amount, 0) AS commission_ttc,
    a.transaction_amount - COALESCE(a.fees_amount, 0) - COALESCE(a.vat_amount, 0) AS reversement
    
FROM
    bizaopartnerdb.main_account_history a
    LEFT JOIN bizaopartnerdb.payment_transaction_log t ON UPPER(t.order_id) = UPPER(a.order_id)
    LEFT JOIN (
        SELECT 
            a.account_number,
            a.date_time,
            v.order_id AS order_id,
            v.operator_transaction_id AS operator_transaction
        FROM 
            bizaopartnerdb.main_account_history a
            LEFT JOIN mobilemoneydb.payment_transactions v ON UPPER(v.order_id) = UPPER(a.order_id)
        WHERE
            DATE(a.date_time) = (CURDATE() - INTERVAL {i} DAY)
            AND DATE(v.transaction_timestamp) = (CURDATE() - INTERVAL {i} DAY)
            AND v.payment_status = 'Successful'
        UNION ALL
        SELECT 
            a.account_number, 
            a.date_time,
            u.order_id AS order_id,
            u.operator_transaction_id AS operator_transaction
        FROM 
            bizaopartnerdb.main_account_history a
            LEFT JOIN visamcdb.payment_transaction u ON UPPER(u.order_id) = UPPER(a.order_id)
        WHERE
            DATE(a.date_time) = (CURDATE() - INTERVAL {i} DAY)
            AND DATE(u.transaction_timestamp) = (CURDATE() - INTERVAL {i} DAY)
            AND u.payment_status = 'Successful'
    ) m ON UPPER(a.order_id) = UPPER(m.order_id)
WHERE
    DATE(a.date_time) = (CURDATE() - INTERVAL {i} DAY)
    AND DATE(t.updated_at) = (CURDATE() - INTERVAL {i} DAY)
ORDER BY 
    a.account_number, a.date_time ASC;
"""

# Replace {i} with the number of days back to query
query = query_template.format(i=4)

# Execute the query and load the result into a DataFrame
df = pd.read_sql(query, source_engine)

 # Sort the DataFrame by 'marchand' and 'date'
df = df.sort_values(by=['marchand', 'date'])

    # Calculate 'balance_before' and 'balance_finish' columns
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

    # Create the Eme_name column
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

    # Calculate 'tva'
df['tva'] = np.where(df['country'] == 'CM', 19.25 / 100, 18 / 100)


# Example: Print the first few rows for verification
print(df.head())                
insert_stmt = text("""
    INSERT INTO pm_merchant_settlement
    (order_id, transaction_id, settlement_name, settlement_date, date, mois, 
    marchand, account_number, currency, country, mno_name, operation_type, 
    volume, fees, transaction_balance, available_balance, taux_marchand, 
    account_type, commission_ttc, reversement, balance_before, balance_finish, 
    eme_name, operator_transaction_id, tva,sub_account_number)
    VALUES
    (:order_id, :transaction_id, :settlement_name, :settlement_date, :date, :mois, 
    :marchand, :account_number, :currency, :country, :mno_name, :operation_type, 
    :volume, :fees, :transaction_balance, :available_balance, :taux_marchand, 
    :account_type, :commission_ttc, :reversement, :balance_before, :balance_finish, 
    :eme_name, :operator_transaction_id, :tva,:sub_account_number)
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
    sub_account_number=EXCLUDED.sub_account_number;
""")

# Use the connection to execute the insert statement
with target_postgres_engine.begin() as connection:
    for index, row in df.iterrows():
        connection.execute(insert_stmt, **row.to_dict())

