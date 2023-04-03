import pandas as pd
import numpy as np
import uuid
from faker import Faker

# Modificar y añadir los campos y registros que se necesiten para probar
def get_dataset(size):
    fake = Faker()
    df = pd.DataFrame()
    df['uuid'] = [str(uuid.uuid4()) for i in range(size)]
    df['position'] = np.random.choice(['up','down','right','left'], size)
    df['age'] = np.random.randint(1, 100, size)
    df['time'] = [fake.date_time_between(start_date='-5y', end_date='+5y') for i in range(size)]
    df['prob'] = np.random.uniform(0, 1, size)
    df['uses'] = np.random.randint(100, 10000, size)
    return df

df = get_dataset(1_000_000)

df.to_csv('random_1M_header.csv', sep=";", index=None)
df.to_csv('random_1M.csv', sep=";", index=None, header=None)
df.to_parquet('random_1M.parquet', index=None)