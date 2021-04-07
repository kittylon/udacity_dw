import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Executes the drop queries included in the variable 
    drop_table_queries
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates the tables in the redshift database, with the queries
    within the variable create_table_queries
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Reads settings from the dwh.cfg file, necessaries for creating 
    a database connection and a cursor, wich will be used to interact
    through queries to the redshift cluster
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()