import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kinesisfirehouse from 'aws-cdk-lib/aws-kinesisfirehose';
import * as logs from 'aws-cdk-lib/aws-logs';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

interface DynamodbStreamingDatalakeStackProps extends cdk.StackProps {
    datalakeBucketName: string,
    datalakeBucketKeyAliasName: string
}

export class DynamodbStreamingDatalakeStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props: DynamodbStreamingDatalakeStackProps) {
        super(scope, id, props);

        const fixedS3Prefix = 'dynamodb/aws21'
        const datalakeBucketKey = kms.Key.fromLookup(this, 'DataLakeKmsKey', { aliasName: props.datalakeBucketKeyAliasName })
        const datalakeBucket = (props?.datalakeBucketName) ? s3.Bucket.fromBucketName(this, 'DatalakeBucket', props.datalakeBucketName) : new s3.Bucket(this, 'DatalakeBucket', {
            bucketName: `dynamodb-streaming-datalake-${cdk.Aws.ACCOUNT_ID}`,
            blockPublicAccess: {
                blockPublicAcls: true,
                blockPublicPolicy: true,
                ignorePublicAcls: true,
                restrictPublicBuckets: true
            }
        })
        const ddbStream = new kinesis.Stream(this, 'DynamoDBStream', {
            streamName: 'ddb-exclusive-stream',
            shardCount: 10,
            encryption: kinesis.StreamEncryption.KMS,
            encryptionKey: datalakeBucketKey
        });

        const exampleDdbTable = new dynamodb.Table(this, 'ExampleDdbTable', {
            tableName: 'example-ddb-table',
            kinesisStream: ddbStream,
            billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
            pointInTimeRecovery: true,
            partitionKey: {
                name: 'id',
                type: dynamodb.AttributeType.STRING
            },
            sortKey: {
                name: 'name',
                type: dynamodb.AttributeType.STRING
            },
            encryption: dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryptionKey: datalakeBucketKey
        });

        const firehoseDeliveryRole = new iam.Role(this, 'FirehoseDeliveryRole', {
            roleName: 'firehose-dbstream-role',
            assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
            description: 'Role for Firehose to deliver data to S3',
            inlinePolicies: {
                ['AllowLogging']: new iam.PolicyDocument({
                    assignSids: true,
                    statements: [
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: [
                                'logs:CreateLogGroup',
                                'logs:CreateLogStream',
                                'logs:PutLogEvents'
                            ],
                            resources: [
                                `arn:${cdk.Aws.PARTITION}:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:/aws/kinesisfirehose/*:*`
                            ]
                        })
                    ]
                }),
                ['KmsPermissions']: new iam.PolicyDocument({
                    assignSids: true,
                    statements: [
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: [
                                'kms:Encrypt',
                                'kms:Decrypt',
                                'kms:ReEncrypt',
                                'kms:GenerateDataKey*',
                                'kms:DescribeKey'
                            ],
                            resources: [
                                datalakeBucketKey.keyArn
                            ]
                        })
                    ]
                }),
                ['S3BucketPermissions']: new iam.PolicyDocument({
                    assignSids: true,
                    statements: [
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: [
                                's3:ListBucket',
                                's3:ListBucketByTags',
                                's3:GetBucketLocation',
                                's3:ListBucketMultipartUploads'
                            ],
                            resources: [
                                datalakeBucket.bucketArn
                            ]
                        })
                    ]
                }),
                ['S3ObjectPermissions']: new iam.PolicyDocument({
                    assignSids: true,
                    statements: [
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: [
                                's3:GetObject',
                                's3:AbortMultipartUpload',
                                's3:PutObject',
                                's3:ListObjects',
                                's3:PutObjectAcl'
                            ],
                            resources: [`${datalakeBucket.bucketArn}/dynamodb/*`]
                        })
                    ]
                }),
                ['KinesisPermissions']: new iam.PolicyDocument({
                    assignSids: true,
                    statements: [
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: [
                                'kinesis:DescribeStream',
                                'kinesis:GetShardIterator',
                                'kinesis:GetRecords',
                                'kinesis:ListShards'
                            ],
                            resources: [
                                ddbStream.streamArn
                            ]
                        })

                    ]
                })
            }
        });
        const firehouseStreamName = 'ddb-table-firehose-delivery-stream';
        const firehouseLogGroup = new logs.LogGroup(this, 'FirehouseLogGroup', {
            logGroupName: `/aws/kinesisfirehose/${firehouseStreamName}`,
            retention: logs.RetentionDays.THREE_MONTHS,
            removalPolicy: cdk.RemovalPolicy.DESTROY
        })
        new kinesisfirehouse.CfnDeliveryStream(this, 'DynamoDBFirehose', {
            deliveryStreamName: firehouseStreamName,
            deliveryStreamType: 'KinesisStreamAsSource',
            kinesisStreamSourceConfiguration: {
                kinesisStreamArn: ddbStream.streamArn,
                roleArn: firehoseDeliveryRole.roleArn
            },
            extendedS3DestinationConfiguration: {
                encryptionConfiguration: {
                    kmsEncryptionConfig: {
                        awskmsKeyArn: datalakeBucketKey.keyArn
                    }
                },
                bucketArn: datalakeBucket.bucketArn,
                bufferingHints: {
                    intervalInSeconds: 60,
                    sizeInMBs: 16
                },
                cloudWatchLoggingOptions: {
                    enabled: true,
                    logGroupName: firehouseLogGroup.logGroupName,
                    logStreamName: 'S3Delivery'
                },
                compressionFormat: 'GZIP',
                errorOutputPrefix: `error/${fixedS3Prefix}/${exampleDdbTable.tableName}/result=!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd/HH}/`,
                prefix: `${fixedS3Prefix}/${exampleDdbTable.tableName}/!{timestamp:yyyy/MM/dd/HH}/`,
                roleArn: firehoseDeliveryRole.roleArn
            }
        })

        new cdk.CfnOutput(this, 'DatalakeBucketArn', { value: datalakeBucket.bucketArn, description: 'Datalake Bucket ARN' });
        new cdk.CfnOutput(this, 'DdbStreamArn', { value: ddbStream.streamArn, description: 'The ARN of the Kinesis Stream for DynamoDB' });
        new cdk.CfnOutput(this, 'DataLakeBucketKeyArn', { value: datalakeBucketKey.keyArn });
        new cdk.CfnOutput(this, 'DdbTableArn', { value: exampleDdbTable.tableArn, description: 'The ARN of the DynamoDB table' });
        new cdk.CfnOutput(this, 'FirehouseRoleArn', { value: firehoseDeliveryRole.roleArn, description: 'The ARN of the Firehose Delivery Role' });
        new cdk.CfnOutput(this, 'FirehouseRoleId', { value: firehoseDeliveryRole.roleId, description: 'The ID of the Firehose Delivery Role' })
    }
}
