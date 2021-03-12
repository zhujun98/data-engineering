"""
Author: Jun Zhu
"""
import configparser
import pprint
import time

import boto3
from botocore.exceptions import ClientError


def create_s3_client():
    s3_client = boto3.client('s3')

    response = s3_client.list_buckets()

    for bucket in response["Buckets"]:
        print(bucket)

    for obj in s3_client.Bucket("operational-space"):
        print(obj)


def create_redshift_security_group(port, group_name):
    ec2 = boto3.client('ec2')

    response = ec2.describe_vpcs()
    vpc_id = response.get('Vpcs', [{}])[0].get('VpcId', '')
    if not vpc_id:
        raise RuntimeError("You must create a VPC first!")

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


def create_redshift_cluster(identifier, *,
                            iam_role_name,
                            db_name,
                            db_user,
                            db_password,
                            db_port,
                            security_group_name):
    iam_client = boto3.client('iam')
    # The IAM role was created by hand.
    role_arns = [
        iam_client.get_role(RoleName=iam_role_name)['Role']['Arn']
    ]

    security_group_id = create_redshift_security_group(
        db_port, security_group_name)

    redshift_client = boto3.client('redshift')
    try:
        response = redshift_client.create_cluster(
            ClusterType="multi-node",
            NodeType="dc2.large",
            NumberOfNodes=2,
            DBName=db_name,
            ClusterIdentifier=identifier,
            MasterUsername=db_user,
            MasterUserPassword=db_password,
            Port=db_port,
            IamRoles=role_arns,
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


def get_redshift_cluster_endpoint(identifier):
    redshift_client = boto3.client('redshift')
    endpoint = redshift_client.describe_clusters(ClusterIdentifier=identifier)[
        'Clusters'][0]['Endpoint']
    return endpoint['Address'], endpoint['Port']


def delete_redshift_cluster(identifier):
    redshift_client = boto3.client('redshift')
    print(f"Deleting Redshift cluster {identifier} ...")
    redshift_client.delete_cluster(ClusterIdentifier=identifier,
                                   SkipFinalClusterSnapshot=True)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config.ini')
    cluster_config = config['CLUSTER']

    create_redshift_cluster(
        cluster_config['IDENTIFIER'],
        iam_role_name=config['IAM_ROLE']['ROLE_NAME'],
        db_name=cluster_config['DB_NAME'],
        db_user=cluster_config['DB_USER'],
        db_password=cluster_config['DB_PASSWORD'],
        db_port=int(cluster_config['DB_PORT']),
        security_group_name=config['VPC']['SECURITY_GROUP_NAME'])

    # delete_redshift_cluster(cluster_config['IDENTIFIER'])
