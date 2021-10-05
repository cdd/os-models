#!/bin/bash

if [[ $# -eq 3 ]]
then
    compile=$1
    validation_class=$2
    name=$3
else
    echo "Usage: $0 yes|no <validation_class> <name>"
    exit 1
fi

echo "compile is $compile, validation class is $validation_class, name is $name"

if [[ $compile == "yes" ]]
then

    echo "Compiling Project"
    mvn -Dmaven.test.skip=true -DskipTests clean install || { echo ' maven build failed'; exit 1; }

    echo "Copying compiled jar to S3"
    aws s3 cp target/uber-os-models-1.0-SNAPSHOT.jar [configured-s3-bucket]/jars/ || { echo 'copy to s3 failed'; exit 1; }

fi

bid_price=1.00
#instance_type=m3.2xlarge
instance_type=m3.xlarge
instance_count=2


aws_command="aws emr create-cluster --applications Name=Ganglia Name=Spark Name=Zeppelin \
--ec2-attributes \"KeyName\"=\"osmodels2\" \
--release-label emr-5.8.0 \
--use-default-roles \
--enable-debugging \
--log-uri 's3n://aws-logs-285083644164-us-west-2/elasticmapreduce/' \
--steps Type=Spark,Name=\"Spark Program $name\",ActionOnFailure=TERMINATE_JOB_FLOW,Args=[\"--deploy-mode\",\"cluster\",--class,com.cdd.spark.validation.$validation_class,[configured-s3-bucket-2]/jars/uber-os-models-1.0-SNAPSHOT.jar] \
--name 'Cluster $name' \
--instance-groups '[{\"InstanceCount\":$instance_count,\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"$instance_type\",\"Name\":\"Core Instance Group\"},{\"InstanceCount\":1,\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"$instance_type\",\"Name\":\"Master Instance Group\"}]' \
--configurations '[{\"Classification\":\"spark\",\"Properties\":{\"maximizeResourceAllocation\":\"true\"},\"Configurations\":[]}]' \
--region us-west-2 \
--auto-terminate \
--no-termination-protected"

aws_command="aws emr create-cluster --applications Name=Ganglia Name=Spark Name=Zeppelin \
--ec2-attributes \"KeyName\"=\"osmodels2\" \
--release-label emr-5.8.0 \
--use-default-roles \
--enable-debugging \
--log-uri 's3n://aws-logs-285083644164-us-west-2/elasticmapreduce/' \
--steps Type=Spark,Name=\"Spark Program $name\",ActionOnFailure=TERMINATE_JOB_FLOW,Args=[\"--deploy-mode\",\"cluster\",--class,com.cdd.spark.validation.$validation_class,[configured-s3-bucket-2]/jars/uber-os-models-1.0-SNAPSHOT.jar] \
--name 'Cluster $name' \
--instance-groups '[{\"InstanceCount\":$instance_count,\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"$instance_type\",\"Name\":\"Core Instance Group\",\"BidPrice\":\"$bid_price\"},{\"InstanceCount\":1,\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"$instance_type\",\"Name\":\"Master Instance Group\",\"BidPrice\":\"$bid_price\"}]' \
--configurations '[{\"Classification\":\"spark\",\"Properties\":{\"maximizeResourceAllocation\":\"true\"},\"Configurations\":[]}]' \
--region us-west-2 \
--auto-terminate \
--no-termination-protected"



echo "AWS command is $aws_command"
eval $aws_command || { echo 'aws create cluster failed'; exit 1; }
