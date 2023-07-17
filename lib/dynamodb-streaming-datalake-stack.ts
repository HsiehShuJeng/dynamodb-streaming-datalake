import * as cdk from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as kinesisfirehouse from 'aws-cdk-lib/aws-kinesisfirehose';
import * as kms from 'aws-cdk-lib/aws-kms';
import { IFunction } from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { JsonProcessor } from './processing-lambda';

interface DynamodbStreamingDatalakeStackProps extends cdk.StackProps {
    datalakeBucketName: string,
    datalakeBucketKeyAliasName: string,
    createNewKmsKey4Kinesis: boolean,
    sameAccountDdbTableName: string
    sameAccountFirehoseRoleName: string,
    sameAccountDdbReadRoleName: string,
    crossAccountGlueJobRoleName: string,
    crossAccountFirehoseRoleName: string,
    crossAccountAccountId: string,
    crossAccountBucketName: string,
    crossAccountBucketKeyId: string
}

export class DynamodbStreamingDatalakeStack extends cdk.Stack {
    fixedS3Prefix: string;
    exampleDdbTable: dynamodb.ITable;
    constructor(scope: Construct, id: string, props: DynamodbStreamingDatalakeStackProps) {
        super(scope, id, props);

        this.fixedS3Prefix = 'dynamodb/aws21';
        let kmsKey4Kinesis: kms.IKey;
        if (props.createNewKmsKey4Kinesis) {
            kmsKey4Kinesis = new kms.Key(this, 'KinesisKmsKey', {
                alias: 'kinesis-exclusive-key',
                description: 'KMS Key for the Kinesis family, i.e., Data Streams and Firehose'
            });
        }
        else {
            kmsKey4Kinesis = kms.Key.fromLookup(this, 'DataLakeKmsKey', { aliasName: props.datalakeBucketKeyAliasName })
        }
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
            encryptionKey: kmsKey4Kinesis
        });
        const jsonProcessor = new JsonProcessor(this, 'JsonProcessor', { kmsKinesisKeyArn: kmsKey4Kinesis.keyArn });

        this.exampleDdbTable = new dynamodb.Table(this, 'ExampleDdbTable', {
            tableName: props.sameAccountDdbTableName,
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
            encryptionKey: kmsKey4Kinesis,
        });

        const firehoseDeliveryRole = new iam.Role(this, 'FirehoseDeliveryRole', {
            roleName: props.sameAccountFirehoseRoleName,
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
                                datalakeBucketKey.keyArn,
                                kmsKey4Kinesis.keyArn
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
                }),
                ['LambdaPermissions']: new iam.PolicyDocument({
                    assignSids: true,
                    statements: [
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: [
                                'lambda:InvokeFunction',
                                'lambda:GetFunctionConfiguration'
                            ],
                            resources: [
                                jsonProcessor.lambdaEntity.functionArn
                            ]
                        })
                    ]
                }),
            }
        });
        const firehouseStreamName = 'ddb-table-firehose-delivery-stream';
        const firehouseLogGroup = new logs.LogGroup(this, 'FirehouseLogGroup', {
            logGroupName: `/aws/kinesisfirehose/${firehouseStreamName}`,
            retention: logs.RetentionDays.THREE_MONTHS,
            removalPolicy: cdk.RemovalPolicy.DESTROY
        })
        const sameAccountDeliveryStream = new kinesisfirehouse.CfnDeliveryStream(this, 'DynamoDBFirehose', {
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
                errorOutputPrefix: `error/${this.fixedS3Prefix}/${this.exampleDdbTable.tableName}/result=!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd/HH}/`,
                prefix: `${this.fixedS3Prefix}/${this.exampleDdbTable.tableName}/!{timestamp:yyyy/MM/dd/HH}/`,
                roleArn: firehoseDeliveryRole.roleArn,
                processingConfiguration: {
                    enabled: true,
                    processors: [{
                        type: 'Lambda',
                        parameters: [{
                            parameterName: 'LambdaArn',
                            parameterValue: jsonProcessor.lambdaEntity.functionArn
                        },
                        {
                            parameterName: 'NumberOfRetries',
                            parameterValue: '2'
                        },
                        {
                            parameterName: 'BufferSizeInMBs',
                            parameterValue: '3'
                        },
                        {
                            parameterName: 'BufferIntervalInSeconds',
                            parameterValue: '60'
                        }]
                    }]
                }
            }
        });
        sameAccountDeliveryStream.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

        const crossAccountFirehoseDeliveryStream = this.createCrossAccountFirehoseDeliveryStream(jsonProcessor.lambdaEntity, ddbStream, kmsKey4Kinesis, props.crossAccountFirehoseRoleName, props.crossAccountAccountId, props.crossAccountBucketName, props.crossAccountBucketKeyId);
        const ddbReadIamRole = new iam.Role(this, 'DdbCrossAccount4Glue', {
            roleName: props.sameAccountDdbReadRoleName,
            assumedBy: new iam.ArnPrincipal(`arn:aws:iam::${props.crossAccountAccountId}:role/${props.crossAccountGlueJobRoleName}`),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName(
                    'AmazonDynamoDBReadOnlyAccess'
                )
            ],
            inlinePolicies: {
                ['DdbKmsPermission']: new iam.PolicyDocument({
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
                                kmsKey4Kinesis.keyArn
                            ]
                        })
                    ]
                })
            }
        });

        new cdk.CfnOutput(this, 'DatalakeBucketArn', { value: datalakeBucket.bucketArn, description: 'Datalake Bucket ARN' });
        new cdk.CfnOutput(this, 'DdbStreamArn', { value: ddbStream.streamArn, description: 'The ARN of the Kinesis Stream for DynamoDB' });
        new cdk.CfnOutput(this, 'SameAccountDeliveryStreamArn', { value: sameAccountDeliveryStream.attrArn, description: 'The ARN of the Firehose Delivery Stream for the same account' });
        new cdk.CfnOutput(this, 'CrossAccountDeliveryStreamArn', { value: crossAccountFirehoseDeliveryStream.attrArn, description: 'The ARN of the Firehose Delivery Stream for the cross account' });
        new cdk.CfnOutput(this, 'DataLakeBucketKeyArn', { value: datalakeBucketKey.keyArn });
        new cdk.CfnOutput(this, 'DdbTableArn', { value: this.exampleDdbTable.tableArn, description: 'The ARN of the DynamoDB table' });
        new cdk.CfnOutput(this, 'FirehouseRoleArn', { value: firehoseDeliveryRole.roleArn, description: 'The ARN of the Firehose Delivery Role' });
        new cdk.CfnOutput(this, 'FirehouseRoleId', { value: firehoseDeliveryRole.roleId, description: 'The ID of the Firehose Delivery Role' });
        new cdk.CfnOutput(this, 'DdbCrossAccountRoleArn', { value: ddbReadIamRole.roleArn, description: 'The ARN of the DynamoDB read role for the cross account' });
    }

    private createCrossAccountFirehoseLogGroup = (firehouseCrossAccountStreamName: string): logs.ILogGroup => {
        return new logs.LogGroup(this, 'FirehouseCrossAccountLogGroup', {
            logGroupName: `/aws/kinesisfirehose/${firehouseCrossAccountStreamName}`,
            retention: logs.RetentionDays.THREE_MONTHS,
            removalPolicy: cdk.RemovalPolicy.DESTROY
        });
    }

    private createCrossAccountFirehoseRole = (jsonProcessor: IFunction, crossAccountFirehoseRoleName: string, kmsKey4Kinesis: kms.IKey, ddbStream: kinesis.IStream, crossAccountAccountId: string, crossAccountBucketName: string): iam.Role => {
        return new iam.Role(this, 'CrossAccountFirehoseRole', {
            roleName: crossAccountFirehoseRoleName,
            assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
            description: 'A role for Firehose to deliver data to S3 for another AWS account.',
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
                                kmsKey4Kinesis.keyArn,
                                `arn:${cdk.Aws.PARTITION}:kms:${cdk.Aws.REGION}:${crossAccountAccountId}:key/*`
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
                                `arn:${cdk.Aws.PARTITION}:s3:::${crossAccountBucketName}`
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
                            resources: [`arn:${cdk.Aws.PARTITION}:s3:::${crossAccountBucketName}/dynamodb/*`]
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
                }),
                ['LambdaPermissions']: new iam.PolicyDocument({
                    assignSids: true,
                    statements: [
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: [
                                'lambda:InvokeFunction',
                                'lambda:GetFunctionConfiguration'
                            ],
                            resources: [
                                jsonProcessor.functionArn
                            ]
                        })
                    ]
                }),
            }
        })
    }

    private createCrossAccountFirehoseDeliveryStream = (jsonProcessor: IFunction, ddbStream: kinesis.IStream, kmsKey4Kinesis: kms.IKey, crossAccountFirehoseRoleName: string, crossAccountAccountId: string, crossAccountBucketName: string, crossAccountBucketKmsKeyId: string): kinesisfirehouse.CfnDeliveryStream => {
        const firehouseCrossAccountStreamName = 'ddb-table-firehose-cross-account-delivery-stream';
        const firehouseCrossAccountLogGroup = this.createCrossAccountFirehoseLogGroup(firehouseCrossAccountStreamName);
        const crossAccountFirehoseRole = this.createCrossAccountFirehoseRole(jsonProcessor, crossAccountFirehoseRoleName, kmsKey4Kinesis, ddbStream, crossAccountAccountId, crossAccountBucketName);
        return new kinesisfirehouse.CfnDeliveryStream(this, 'DynamoDBCrossAccountFirehose', {
            deliveryStreamName: firehouseCrossAccountStreamName,
            deliveryStreamType: 'KinesisStreamAsSource',
            kinesisStreamSourceConfiguration: {
                kinesisStreamArn: ddbStream.streamArn,
                roleArn: crossAccountFirehoseRole.roleArn
            },
            extendedS3DestinationConfiguration: {
                bucketArn: `arn:${cdk.Aws.PARTITION}:s3:::${crossAccountBucketName}`,
                bufferingHints: {
                    intervalInSeconds: 60,
                    sizeInMBs: 16
                },
                encryptionConfiguration: {
                    kmsEncryptionConfig: {
                        awskmsKeyArn: `arn:aws:kms:${cdk.Aws.REGION}:${crossAccountAccountId}:key/${crossAccountBucketKmsKeyId}`
                    }
                },
                cloudWatchLoggingOptions: {
                    enabled: true,
                    logGroupName: firehouseCrossAccountLogGroup.logGroupName,
                    logStreamName: 'S3Delivery'
                },
                compressionFormat: 'GZIP',
                errorOutputPrefix: `error/${this.fixedS3Prefix}/${this.exampleDdbTable.tableName}/result=!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd/HH}/`,
                prefix: `${this.fixedS3Prefix}/${this.exampleDdbTable.tableName}/!{timestamp:yyyy/MM/dd/HH}/`,
                roleArn: crossAccountFirehoseRole.roleArn,
                processingConfiguration: {
                    enabled: true,
                    processors: [{
                        type: 'Lambda',
                        parameters: [{
                            parameterName: 'LambdaArn',
                            parameterValue: jsonProcessor.functionArn
                        },
                        {
                            parameterName: 'NumberOfRetries',
                            parameterValue: '2'
                        },
                        {
                            parameterName: 'BufferSizeInMBs',
                            parameterValue: '3'
                        },
                        {
                            parameterName: 'BufferIntervalInSeconds',
                            parameterValue: '60'
                        }]
                    }]
                }
            }
        });
    }
}
