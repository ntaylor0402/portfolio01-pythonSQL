import pandas as pd
import psycopg2 as pg

def main():
    """runs one instance of a script that automates entry of a csv dataset into a PostgresQL table"""
    # prepare data as pandas dataframe
    df = pd.read_csv('UNRATE.csv')
    df = reformat_date(df)
    
    # connect to PostgreSQL database
    connection = pg.connect(host='', dbname='portfolio', user='postgres', password = 'password', port=5432)
    cursor = connection.cursor()   
    
    # prepare SQL syntax to execute
    sql_statement = generate_sql_statement(df)

    # Execute SQL statement
    cursor.execute(sql_statement)

    # Commit and Close
    connection.commit()
    cursor.close()
    connection.close()

      
def reformat_date(df):
    """reformats the composite date string column into seperate 'YEAR' and 'MONTH' columns"""
    years = [date[0:4] for date in df['DATE'].values]
    months = [date[5:7] for date in df['DATE'].values]

    df.insert(1, 'YEAR', years)
    df.insert(2, 'MONTH', months)

    df = df.drop('DATE', axis=1)

    return df

def generate_sql_statement(df):
    """generates SQL INSERT statement string to pass to psycopg2"""
    
    values= []
    for i in range(len(df)):
        
        year = (df['YEAR'][i])
        month = (df['MONTH'][i])
        unemployment_rate = (df['UNRATE'][i])

        value = (year, month, unemployment_rate)
        values.append(str(value))
    
    values = ', '.join(values)
    return (f'INSERT INTO unemployment_rate (year, month, unemployment_rate) VALUES {values};')


if __name__ == '__main__':
    main()
