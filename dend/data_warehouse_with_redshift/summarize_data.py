import boto3

from run_on_aws import s3_config


def check_data_on_s3():
    client = boto3.client('s3', region_name=s3_config['REGION'])

    response = client.list_objects(Bucket=s3_config['BUCKET'],
                                   Prefix='log_data/')['Contents']
    # subtract the root folder itself
    print("Number of log data files: ", len(response) - 1)
    print("The first log data file: ", response[1]['Key'])
    print("The last log data file: ", response[-1]['Key'])

    print()

    response = client.list_objects(Bucket=s3_config['BUCKET'],
                                   Prefix='song_data/')['Contents']
    print("Number of song data files: ", len(response) - 1)
    print("The first song data file: ", response[1]['Key'])
    print("The last song data file: ", response[-1]['Key'])


if __name__ == "__main__":

    check_data_on_s3()
