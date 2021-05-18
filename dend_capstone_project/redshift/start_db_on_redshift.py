"""
Author: Jun Zhu
"""
import argparse
import configparser
import pprint
import time

import psycopg2

import boto3
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read('config.ini')
cluster_config = config['CLUSTER']


def create_redshift_security_group():
    ec2 = boto3.client('ec2')

    # Each region has a unique VPC.
    response = ec2.describe_vpcs()
    vpc_id = response.get('Vpcs', [{}])[0].get('VpcId', '')
    if not vpc_id:
        raise RuntimeError("You must create a VPC first!")

    port = int(cluster_config['DB_PORT'])
    group_name = config['VPC']['SECURITY_GROUP_NAME']
    try:
        response = ec2.create_security_group(
            GroupName=group_name,
            Description='redshift security group',
            VpcId=vpc_id)
        security_group_id = response['GroupId']
        print(f"Security Group {security_group_id} Created in vpc {vpc_id}.")

        data = ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {'IpProtocol': 'tcp',
                 'FromPort': port,
                 'ToPort': port,
                 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
            ])
        print(f"Ingress Successfully Set {data}")
        return security_group_id
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidGroup.Duplicate':
            response = ec2.describe_security_groups(
                Filters=[
                    dict(Name='group-name', Values=[group_name])
                ]
            )
            return response['SecurityGroups'][0]['GroupId']
        raise e


def get_iam_role_arns():
    iam_client = boto3.client('iam')
    # The IAM role was created by hand.
    iam_role_name = config['IAM_ROLE']['ROLE_NAME']
    role_arns = [
        iam_client.get_role(RoleName=iam_role_name)['Role']['Arn']
    ]
    return role_arns


def create_redshift_cluster():
    identifier = cluster_config['IDENTIFIER']
    security_group_id = create_redshift_security_group()
    iam_role_arns = get_iam_role_arns()

    redshift_client = boto3.client('redshift')
    try:
        response = redshift_client.create_cluster(
            ClusterType="multi-node",
            NodeType=cluster_config['NODE_TYPE'],
            NumberOfNodes=int(cluster_config['NODE_COUNT']),
            DBName=cluster_config['DB_NAME'],
            ClusterIdentifier=identifier,
            MasterUsername=cluster_config['DB_USER'],
            MasterUserPassword=cluster_config['DB_PASSWORD'],
            Port=int(cluster_config['DB_PORT']),
            IamRoles=iam_role_arns,
            VpcSecurityGroupIds=[security_group_id],
        )
    except ClientError as e:
        response = None
        if e.response['Error']['Code'] != 'ClusterAlreadyExists':
            raise e

    # Wait until the status of the cluster becomes 'available'.
    print("Creating Redshift cluster ...")
    while True:
        info = redshift_client.describe_clusters(ClusterIdentifier=identifier)[
            'Clusters'][0]

        if info['ClusterStatus'] == 'available':
            pprint.PrettyPrinter().pprint(info)
            break
        else:
            time.sleep(10)

    return response


def get_redshift_cluster_endpoint():
    redshift_client = boto3.client('redshift')
    endpoint = redshift_client.describe_clusters(
        ClusterIdentifier=cluster_config['IDENTIFIER'])[
        'Clusters'][0]['Endpoint']
    return endpoint['Address'], endpoint['Port']


def delete_redshift_cluster():
    identifier = cluster_config['IDENTIFIER']

    redshift_client = boto3.client('redshift')
    print(f"Deleting Redshift cluster {identifier} ...")
    redshift_client.delete_cluster(ClusterIdentifier=identifier,
                                   SkipFinalClusterSnapshot=True)


def create_tables():
    from sql_queries import create_table_queries

    address, port = get_redshift_cluster_endpoint()
    # connect to default database
    print(f"Connecting to Redshift cluster at {address}:{port} ...")
    conn = psycopg2.connect(
        f"host={address} "
        f"dbname={cluster_config['DB_NAME']} "
        f"user={cluster_config['DB_USER']} "
        f"password={cluster_config['DB_PASSWORD']} "
        f"port={port}")

    cur = conn.cursor()
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--delete", action="store_true")
    args = parser.parse_args()

    if args.delete:
        delete_redshift_cluster()
    else:
        # create_redshift_cluster()
        create_tables()
