import configparser
import psycopg2
from sql_queries import create_table_queries

""" 
Function creates all the tables   
 
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print('Connecting to redshift')
    #con= psycopg2.connect(dbname= 'immigration', host='redshift-cluster-1.ckxqlugu6vhf.us-east-1.redshift.amazonaws.com',       port= 5439, user= 'redshiftuser', password= 'Qazwsx!234!')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to redshift')
    cur = conn.cursor()
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()