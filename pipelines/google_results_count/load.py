import pandas as pd

def write_to_csv(df, filepath):
    print('_'*42, f'\nExport data, dimension: {df.shape} to\t{filepath}\n')
    print(df.head(2).to_markdown())
    df.to_csv(f'{filepath}', index=False) 
