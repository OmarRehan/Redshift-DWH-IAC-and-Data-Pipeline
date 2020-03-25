import configparser
import psycopg2
from sql_queries import *
from IAC_create_redshift_cluster import func_connect_to_redshift
import logging
import sys


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    try:
        # Connecting to the Cluster
        conn, cur = func_connect_to_redshift('dwh.cfg')

        dict_conn_info = conn.get_dsn_parameters()

        logging.info(" Connected to {}, Host: {}, User: {}".format \
                (
                dict_conn_info.get('dbname')
                , dict_conn_info.get('host')
                , dict_conn_info.get('user')
            )
        )
        
    except Exception as e:
        logging.error(" Faild to Connect to the Cluster, {}".format(e))
        sys.exit(-1)
        
    
    try :
        
        # Copying the Data from S3 to the Redshift Cluster
        logging.info(' Copying the Data from S3 to the Redshift Cluster')
        
        for stg_table_name,copy_cmd in copy_table_queries.items():
            cur.execute(copy_cmd)
            conn.commit()
            logging.info(' Copying data to {} has completed Successfully.'.format(stg_table_name))
        
    except Exception as e:
        logging.error(" Faild to Copy the Data to the Cluster, {}".format(e))
        conn.close()
        sys.exit(-1)
        
        
    try :
        
        # Loading the Data into the Main Tables
        logging.info(' Loading the Data into the Main Tables.')
        
        for table_name,load_query in insert_table_queries.items():
            cur.execute(load_query)
            int_row_count = cur.rowcount
            cur.execute("END TRANSACTION;")
            conn.commit()
            logging.info(' {} Records have been Merged into {}.'.format(int_row_count,table_name))
            
            
        conn.close()
        
    except Exception as e:
        logging.error(" Faild to Load the Data to the Cluster, {}".format(e))
        conn.close()
        sys.exit(-1)
        


    
    
    

if __name__ == "__main__":
    main()