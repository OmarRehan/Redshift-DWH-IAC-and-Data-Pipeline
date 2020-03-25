import boto3
import configparser
import logging
import sys
import json
import time

logging.basicConfig(level=logging.INFO,format="%(asctime)s: %(levelname)s:%(message)s")
        

if __name__ == '__main__':
    
    # Reading the Configurations File
    try:
        
        logging.info(' Reading the Configurations File')
        
        config_ = configparser.ConfigParser()
        config_.read('dwh.cfg')
        
        config_access_key = config_.get('IAM_ROLE','ACCESS_KEY')
        config_access_secret = config_.get('IAM_ROLE','ACCESS_SECRET')
        config_dwh_IAM_role = config_.get('IAM_ROLE','DWH_IAM_ROLE_NAME')
        config_dwh_cluster_identifier = config_.get('CLUSTER','DWH_CLUSTER_IDENTIFIER')
        config_dwh_vpc_id = config_.get('CLUSTER','dwh_vpc_id')
        
    except Exception as e:
        logging.error(' Failed to process the Configurations File, {}'.format(e))
        sys.exit(-1)
    
    
    # Deleting the Redshift Cluster
    try:
        
        logging.info(" Deleting the Redshift Cluster")
        
        # Initializing a redshift resource to control the Cluster
        client_redshift = boto3.client('redshift',region_name="us-west-2",aws_access_key_id=config_access_key,aws_secret_access_key=config_access_secret)
        
        str_cluster_status = client_redshift.describe_clusters(ClusterIdentifier=config_dwh_cluster_identifier)['Clusters'][0].get('ClusterStatus')

        if str_cluster_status == 'available':
            client_redshift.delete_cluster( ClusterIdentifier=config_dwh_cluster_identifier,  SkipFinalClusterSnapshot=True)
            
            str_cluster_status = client_redshift.describe_clusters(ClusterIdentifier=config_dwh_cluster_identifier)['Clusters'][0].get('ClusterStatus')

            while str_cluster_status == 'deleting':
                logging.info(' Deleting the Cluster')
                time.sleep(5)
                try:
                    str_cluster_status = client_redshift.describe_clusters(ClusterIdentifier=config_dwh_cluster_identifier)['Clusters'][0].get('ClusterStatus')
                except Exception as e:
                    break

            logging.info(" The Redshift Cluster has been deleted")
            
        else:
            logging.warn(" The Redshift Cluster is Not Available")
        

        
    except Exception as e:
        logging.error(' Failed to Delete The redshift Cluster, {}'.format(e))
        #sys.exit(-1)
        
        
    # Delteing the IAM Role
    try:
        
        client_iam = boto3.client('iam',aws_access_key_id=config_access_key,aws_secret_access_key=config_access_secret,region_name='us-west-2')
        
        client_iam.detach_role_policy(RoleName=config_dwh_IAM_role, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        
        client_iam.delete_role(RoleName=config_dwh_IAM_role)
        
        logging.info(' IAM Role {} has been Deleted.'.format(config_dwh_IAM_role))
        
    except Exception as e:
        logging.warn(' Failed to Delete The IAM Role, {}'.format(e))

    
    # Deleting the VPC
    try:
        
        client_ec2 = boto3.resource('ec2',region_name="us-west-2",aws_access_key_id=config_access_key,aws_secret_access_key=config_access_secret)
        ec2_client_meta = client_ec2.meta.client
        ec2_client_meta.delete_vpc(VpcId = config_dwh_vpc_id)
        
        logging.info(' VPC has been Deleted.'.format(config_dwh_IAM_role))
        
    except Exception as e:
        logging.warn(' Failed to Delete The VPC, {}'.format(e))

