import boto3
import configparser
import logging
import sys
import json
import time
import psycopg2

logging.basicConfig(level=logging.INFO,format="%(asctime)s: %(levelname)s:%(message)s")

def func_create_IAM_role(param_config_access_key,param_config_access_secret,param_iam_role_name):
    """Initalizing IAM Role mainly to be used by the redshift cluster to access S3 Buckets"""
    
    iam = boto3.client(
        'iam',
        aws_access_key_id=config_access_key,
        aws_secret_access_key=config_access_secret,
        region_name='us-west-2'
    )

    dwhRole = iam.create_role(
        Path='/',
        RoleName=param_iam_role_name,
        AssumeRolePolicyDocument=json.dumps(
            {
                'Statement': 
                [
                    {
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {'Service': 'redshift.amazonaws.com'}
                    }
                ],
             'Version': '2012-10-17'
            }
        )
    )

    iam.attach_role_policy(RoleName=param_iam_role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    return iam.get_role(RoleName=param_iam_role_name)['Role']['Arn']
    

def func_connect_to_redshift(param_config_path):
    # Reading the Cluster Configurations
    config = configparser.ConfigParser()
    config.read(param_config_path)
    
    config_db_name = config.get('CLUSTER','db_name')
    config_db_user = config.get('CLUSTER','db_user')
    config_db_password = config.get('CLUSTER','db_password')
    config_dwh_end_point = config.get('CLUSTER','dwh_end_point')
    config_db_port = config.get('CLUSTER','db_port')
    
    # Establishing a Connection to the Cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config_dwh_end_point,config_db_name,config_db_user,config_db_password,config_db_port))
    cur = conn.cursor()
    
    return conn,cur


if __name__ == '__main__':
    
    # Reading the Configurations File
    try:
        
        logging.info(' Reading the Configurations File')
        
        config_ = configparser.ConfigParser()
        config_.read('dwh.cfg')
        
        config_access_key = config_.get('IAM_ROLE','ACCESS_KEY')
        config_access_secret = config_.get('IAM_ROLE','ACCESS_SECRET')
        config_dwh_IAM_role = config_.get('IAM_ROLE','DWH_IAM_ROLE_NAME')
        config_cluster_identifier = config_.get('CLUSTER','DWH_CLUSTER_IDENTIFIER')
        config_cluster_type = config_.get('CLUSTER','DWH_CLUSTER_TYPE')
        config_node_type = config_.get('CLUSTER','DWH_NODE_TYPE')
        config_num_of_nodes = config_.get('CLUSTER','DWH_NUM_NODES')
        config_db_name = config_.get('CLUSTER','DB_NAME')
        config_db_user = config_.get('CLUSTER','DB_USER')
        config_db_password = config_.get('CLUSTER','DB_PASSWORD')
        config_db_port = config_.get('CLUSTER','DB_PORT')
        
        
    except Exception as e:
        logging.error(' Failed to process the Configurations File, {}'.format(e))
        sys.exit(-1)
        
    
    # Creating IAM Role fr the Redshift Cluster
    try:
        
        logging.info(' Creating an IAM Role for the Redshift Cluster')
        
        ARN_redshift_IAM_role = func_create_IAM_role(config_access_key,config_access_secret,config_dwh_IAM_role)

        config_['CLUSTER']['DWH_ARN'] = ARN_redshift_IAM_role
        
    except Exception as e:
        logging.error(' Failed to create The IAM Role, {}'.format(e))
        sys.exit(-1)
    
    
        
    # Creating the Redshift Cluster
    try:
        
        client_redshift = boto3.client('redshift',region_name="us-west-2",aws_access_key_id=config_access_key,aws_secret_access_key=config_access_secret)
        
        logging.info(" Initializing redshift Client")
        
        dict_cluster_info = client_redshift.create_cluster(      
                ClusterType=config_cluster_type,
                NodeType=config_node_type,
                NumberOfNodes=int(config_num_of_nodes),

                #Identifiers & Credentials
                DBName=config_db_name,
                ClusterIdentifier=config_cluster_identifier,
                MasterUsername=config_db_user,
                MasterUserPassword=config_db_password,

                IamRoles = [ARN_redshift_IAM_role]  
            )
        
        logging.debug(dict_cluster_info)
            
        config_vpc_id = dict_cluster_info.get('Cluster').get('VpcId')
        #config_vpc_security_grp = dict_cluster_info.get('Cluster').get('VpcSecurityGroups')[0]
        
        str_cluster_status = client_redshift.describe_clusters(ClusterIdentifier=config_cluster_identifier)['Clusters'][0].get('ClusterStatus')
        
        while str_cluster_status == 'creating':
            logging.info(' Creating the Cluster')
            time.sleep(5)
            str_cluster_status = client_redshift.describe_clusters(ClusterIdentifier=config_cluster_identifier)['Clusters'][0].get('ClusterStatus')
            
        logging.info(" The Redshift Cluster has been Created")
    

        
    except Exception as e:
        logging.error(' Failed to create The Redshift Cluster, {}'.format(e))
        sys.exit(-1)
    
    
    try:
        
        logging.info('Updating the Configurations File')
        
        dict_cluster_info = client_redshift.describe_clusters(ClusterIdentifier=config_cluster_identifier)['Clusters'][0]

        str_cluster_status = dict_cluster_info.get('ClusterStatus')
        
        str_cluster_end_point = dict_cluster_info.get('Endpoint').get('Address')

        logging.debug(str_cluster_end_point)
        logging.debug(config_vpc_id)
        
        config_['CLUSTER']['DWH_END_POINT'] = str_cluster_end_point
        config_['CLUSTER']['DWH_VPC_ID'] = config_vpc_id
        
        with open('dwh.cfg', 'w') as configfile:
            config_.write(configfile)
        
        configfile.close()
        
    except Exception as e:
        logging.error(' Failed to create the Configuration File, {}'.format(e))
        configfile.close()
        sys.exit(-1)
    
    
    
    # Creating a VPC via EC2 
    try:
        
        logging.info(' Initializing VPC service')
        
        client_ec2 = boto3.resource('ec2',region_name="us-west-2",aws_access_key_id=config_access_key,aws_secret_access_key=config_access_secret)
        
        client_vpc = client_ec2.Vpc(id=config_vpc_id)
        
        defaultSg = list(client_vpc.security_groups.all())[0]

        defaultSg.authorize_ingress(GroupName=defaultSg.group_name,CidrIp='0.0.0.0/0',IpProtocol='TCP',FromPort=int(config_db_port),ToPort=int(config_db_port))
        
    except Exception as e:
        logging.warn(' Failed to create The VPC Service, {}'.format(e))
        

        
