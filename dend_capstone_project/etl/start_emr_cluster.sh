aws emr create-cluster --release-label emr-5.29.0 \
                       --instance-type m5.xlarge \
                       --instance-count 3 \
                       --name capstone \
                       --use-default-roles \
                       --applications Name=Spark Name=Livy \
                       --ec2-attributes KeyName=spark_emr