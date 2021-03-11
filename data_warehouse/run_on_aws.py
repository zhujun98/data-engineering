"""
Author: Jun Zhu
"""
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


def create_redshift_cluster(identifier='dwhCluster', db_name='dwh'):
    iam_client = boto3.client('iam')
    # The IAM role was created by hand.
    role_arns = [
        iam_client.get_role(RoleName='RedshiftRole')['Role']['Arn']
    ]

    redshift_client = boto3.client('redshift')
    try:
        response = redshift_client.create_cluster(
            ClusterType="multi-node",
            NodeType="dc2.large",
            NumberOfNodes=2,

            # Identifiers & Credentials
            DBName=db_name,
            ClusterIdentifier=identifier,
            MasterUsername="dwhuser",
            MasterUserPassword="RedshiftPass123",

            IamRoles=role_arns,
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


def delete_redshift_cluster(identifier='dwhCluster'):
    redshift_client = boto3.client('redshift')
    print(f"Deleting Redshift cluster {identifier} ...")
    redshift_client.delete_cluster(ClusterIdentifier=identifier,
                                   SkipFinalClusterSnapshot=True)


if __name__ == "__main__":
    create_redshift_cluster()

    delete_redshift_cluster()
