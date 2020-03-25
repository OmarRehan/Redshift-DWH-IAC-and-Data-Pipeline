import configparser
import psycopg2
from sql_queries import *
from IAC_create_redshift_cluster import func_connect_to_redshift
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s")


def drop_tables(cur, conn,param_query_list):
    for query in drop_table_queries:
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
    
    
    try:
        # Creating the Schemas
        cur.execute(create_schema_staging)
        conn.commit()
        logging.info(' {} has been created'.format(dict_DDL_schemas.get('schema_staging_name')))
        
        cur.execute(create_scehma_sparkify)
        conn.commit()
        logging.info(' {} has been created'.format(dict_DDL_schemas.get('schema_sparkify_name')))
        
    except Exception as e:
        logging.error(" Faild to Create the Tables, {}".format(e))
        conn.close()
        sys.exit(-1)
        
    
    try :
        
        # Creating the Tables
        drop_tables(cur,conn,drop_table_queries)
        
        for table_name,table_query in create_table_queries.items():
            cur.execute(table_query)
            conn.commit()
            logging.info(' {} has been created.'.format(table_name))

        conn.close()
    
    except Exception as e:
        logging.error(" Faild to Create the Tables, {}".format(e))
        conn.close()
        sys.exit(-1)


if __name__ == "__main__":
    main()