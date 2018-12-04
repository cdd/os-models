
Technical notes to assist in running Apache Spack jobs on AWS using Amazon's
EMR service.


#### Spark EC2

This is no longer included in spark distribution- get it from github.

`git clone https://github.com/amplab/spark-ec2.git`

`git checkout branch-2.0`

#### Security/AWS MFA

os-models key pair created:

```
export AWS_SECRET_ACCESS_KEY=<SECRET_ACCESS_KEY>
export AWS_ACCESS_KEY_ID=<ACCESS_KEY_ID>
```

With MFA this allows you to request a session token:

```
assigned mfa device arn:aws:iam::123456789012:mfa/user
```

see `http://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html` and `https://aws.amazon.com/premiumsupport/knowledge-center/authenticate-mfa-cli/`

```
$ aws sts get-session-token --serial-number arn:aws:iam::123456789012:mfa/user --token-code code-from-token
{
"Credentials": {
    "SecretAccessKey": "secret-access-key",
    "SessionToken": "temporary-session-token",
    "Expiration": "expiration-date-time",
    "AccessKeyId": "access-key-id"
    ;}
}
```

where the token code is from the Authy app

Then update ~/.aws/credentials.  If you do this you will need to reset credentials to the original values when you next try to get a session token.

```
$ cat ~/.aws/credentials
[default]
aws_access_key_id = <Access-key-as-in-returned-output>
aws_secret_access_key = <Secret-access-key-as-in-returned-output>
aws_session_token = <Session-Token-as-in-returned-output>
```

or set environment variables (this may be better as you don't need to worry about resetting the credentials file)

```
export AWS_ACCESS_KEY_ID=<Access-key-as-in-returned-output>
export AWS_SECRET_ACCESS_KEY=<Secret-access-key-as-in-returned-output>
export AWS_SESSION_TOKEN=<Session-Token-as-in-returned-output>
```

This process is automated in com.cdd.bin.AwsEnvironment, with takes the token code and prints out export command for the environment variables which can be copied and pasted.

#### Web access

Allow ssh access in security group

Follow Amazon instructions to enable Foxy Proxy/port forwarding and to access web UIs

Accessing spark web ui:

(port forwarding is also required to view logs)

see `http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-history.html` and `http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-web-interfaces.html`, set up foxy proxy and connect e.g.

Set up port forwarding:

```
ssh -i ~/.ssh/osmodels.pem -ND 8157 hadoop@<hadoop-cluster-ip>
```

Then connect

```
http://<hadoop-cluster-ip>:18080
```

To get spark UI connect to Yarn ui on port 8088 and choose ApplicationMaster link under tracking UI


#### S3

Aws copy jar-file
```
aws s3 cp target/uber-os-models-1.0-SNAPSHOT.jar s3://os-models/jars/
```

Aws pull results (in pipeline results)
```
aws s3 sync s3://os-models/data/pipeline_results .`
```

Or download results
```
aws s3  cp s3://os-models/data/pipeline_results/tune_glr_desc.parquet data/pipeline_results/tune_glr_desc.parquet --recursive
```

Upload parquet data directory:

```
aws s3  cp ADMECaco2.parquet s3://os-models/data/continuous/ADMECaco2.parquet --recursive
```

#### Resourcing Spark

Adding step to spark cluster add `--class com.cdd.models.pipeline.tuning.TuneRandomForest` as spark submit options


Simple CLI to start cluster with max resources:

```
aws emr create-cluster --release-label emr-5.6.0 --applications Name=Spark \
--instance-type m4.xlarge --instance-count 5 --service-role EMR_DefaultRole \
--ec2-attributes '{"KeyName":"osmodels","InstanceProfile":"EMR_EC2_DefaultRole"}' \
--configurations file://./myConfig.json  --region us-west-2
```

```
$ cat myConfig.json
[
  {
    "Classification": "spark",
    "Properties": {
      "maximizeResourceAllocation": "true"
    }
  }
]
```

Links for allocating resources:
`https://aws.amazon.com/blogs/big-data/submitting-user-applications-with-spark-submit/`
`http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html`


```
aws emr create-cluster --applications Name=Spark --ec2-attributes '{"KeyName":"osmodels","InstanceProfile":"EMR_EC2_DefaultRole","AvailabilityZone":"us-west-2a","EmrManagedSlaveSecurityGroup":"sg-e809c692","EmrManagedMasterSecurityGroup":"sg-ca0dc2b0"}' --service-role EMR_DefaultRole --release-label emr-5.6.0 --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--class","com.cdd.models.pipeline.tuning.TuneRandomForest","s3://os-models/jars/uber-os-models-1.0-SNAPSHOT.jar"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark application"}]' --name 'Development Cluster' --instance-groups '[{"InstanceCount":4,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"CORE"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"MASTER"}]' --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' --scale-down-behavior TERMINATE_AT_INSTANCE_HOUR --region us-west-2
```

#### AWS command for GBT

In a new terminal `cdd_cmd AwsEnvironment 38123` (use the appropriate token).  Then copy and pase environment variables.

Build jar and copy to AWS:

```
aws s3 cp target/uber-os-models-1.0-SNAPSHOT.jar s3://os-models/jars/
```


Create cluster (deploy mode must be cluster or remote jar will not be loaded).  This command works and is a good reference for running other tuning classes

```
aws emr create-cluster --applications Name=Ganglia Name=Spark Name=Zeppelin \
--ec2-attributes "KeyName"="osmodels" \
--release-label emr-5.7.0 \
--use-default-roles \
--enable-debugging \
--log-uri 's3n://aws-logs-688102026996-us-west-2/elasticmapreduce/' \
--steps Type=Spark,Name="Spark Program GLR Tune",ActionOnFailure=TERMINATE_JOB_FLOW,Args=["--deploy-mode","cluster",--class,com.cdd.spark.validation.TuneGradientBoostedTreesRegression,s3://os-models/jars/uber-os-models-1.0-SNAPSHOT.jar] \
--name 'My cluster' \
--instance-groups '[{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core Instance Group"},\
{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master Instance Group"}]' \
--configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' \
--region us-west-2 \
--auto-terminate \
--no-termination-protected
```

Same thing, but using spot instances:

```

aws emr create-cluster --applications Name=Ganglia Name=Spark Name=Zeppelin \
--ec2-attributes "KeyName"="osmodels" \
--release-label emr-5.7.0 \
--use-default-roles \
--enable-debugging \
--log-uri 's3n://aws-logs-688102026996-us-west-2/elasticmapreduce/' \
--steps Type=Spark,Name="Spark Program GLR Tune",ActionOnFailure=TERMINATE_JOB_FLOW,Args=["--deploy-mode","cluster",--class,com.cdd.spark.validation.TuneGradientBoostedTreesRegression,s3://os-models/jars/uber-os-models-1.0-SNAPSHOT.jar] \
--name 'My cluster' \
--instance-groups '[{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core Instance Group","BidPrice":"0.75"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master Instance Group","BidPrice":"0.75"}]' \
--configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' \
--region us-west-2 \
--auto-terminate \
--no-termination-protected
```

#### Aws command for Naive Bayes

```

aws emr create-cluster --applications Name=Ganglia Name=Spark Name=Zeppelin \
--ec2-attributes "KeyName"="osmodels" \
--release-label emr-5.7.0 \
--use-default-roles \
--enable-debugging \
--log-uri 's3n://aws-logs-688102026996-us-west-2/elasticmapreduce/' \
--steps Type=Spark,Name="Spark Program RLC Tune",ActionOnFailure=TERMINATE_JOB_FLOW,Args=["--deploy-mode","cluster",--class,com.cdd.spark.validation.TuneNaiveBayes,s3://os-models/jars/uber-os-models-1.0-SNAPSHOT.jar] \
--name 'My cluster NB' \
--instance-groups '[{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core Instance Group","BidPrice":"0.75"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master Instance Group","BidPrice":"0.75"}]' \
--configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' \
--region us-west-2 \
--auto-terminate \
--no-termination-protected
```

#### AWS command for Logistic Regression

```

aws emr create-cluster --applications Name=Ganglia Name=Spark Name=Zeppelin \
--ec2-attributes "KeyName"="osmodels" \
--release-label emr-5.7.0 \
--use-default-roles \
--enable-debugging \
--log-uri 's3n://aws-logs-688102026996-us-west-2/elasticmapreduce/' \
--steps Type=Spark,Name="Spark Program LRC Tune",ActionOnFailure=TERMINATE_JOB_FLOW,Args=["--deploy-mode","cluster",--class,com.cdd.spark.validation.TuneLogisiticRegressionClassifier,s3://os-models/jars/uber-os-models-1.0-SNAPSHOT.jar] \
--name 'My cluster LRC' \
--instance-groups '[{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core Instance Group","BidPrice":"0.75"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master Instance Group","BidPrice":"0.75"}]' \
--configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' \
--region us-west-2 \
--auto-terminate \
--no-termination-protected
```

#### AWS command for tune linear regression

```
aws emr create-cluster --applications Name=Ganglia Name=Spark Name=Zeppelin \
--ec2-attributes "KeyName"="osmodels" \
--release-label emr-5.7.0 \
--use-default-roles \
--enable-debugging \
--log-uri 's3n://aws-logs-688102026996-us-west-2/elasticmapreduce/' \
--steps Type=Spark,Name="Spark Program LR Tune",ActionOnFailure=TERMINATE_JOB_FLOW,Args=["--deploy-mode","cluster",--class,com.cdd.spark.validation.TuneLinearRegression,s3://os-models/jars/uber-os-models-1.0-SNAPSHOT.jar] \
--name 'My cluster Linear Regression' \
--instance-groups '[{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core Instance Group","BidPrice":"0.75"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master Instance Group","BidPrice":"0.75"}]' \
--configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' \
--region us-west-2 \
--auto-terminate \
--no-termination-protected
```

#### AWS command for tune linear regression on descriptors

```
aws emr create-cluster --applications Name=Ganglia Name=Spark Name=Zeppelin \
--ec2-attributes "KeyName"="osmodels" \
--release-label emr-5.7.0 \
--use-default-roles \
--enable-debugging \
--log-uri 's3n://aws-logs-688102026996-us-west-2/elasticmapreduce/' \
--steps Type=Spark,Name="Spark Program LR Descriptors Tune",ActionOnFailure=TERMINATE_JOB_FLOW,Args=["--deploy-mode","cluster",--class,com.cdd.spark.validation.TuneLinearRegressionDescriptors,s3://os-models/jars/uber-os-models-1.0-SNAPSHOT.jar] \
--name 'My cluster Linear Regression on Descriptors' \
--instance-groups '[{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core Instance Group","BidPrice":"0.75"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master Instance Group","BidPrice":"0.75"}]' \
--configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' \
--region us-west-2 \
--auto-terminate \
--no-termination-protected
```

#### Shell command

AWS create cluster commands replaced by submit-aws.sh script in bin directory
