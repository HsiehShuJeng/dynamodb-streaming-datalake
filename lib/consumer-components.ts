import * as cdk from 'aws-cdk-lib';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

interface ConsumerStackProps extends cdk.StackProps {
    readonly producerAccountId: string;
    readonly producerFirehoseRoleName: string;
    readonly datalakeBucketName: string;
    readonly producerDdbReadRoleName: string;
    readonly producerDdbTableName: string;
}

export class ConsumerStack extends cdk.Stack {
    fixedS3Prefix: string;
    constructor(scope: Construct, id: string, props: ConsumerStackProps) {
        super(scope, id, props);
        this.fixedS3Prefix = 'dynamodb/aws21';
        const producerAccountId = props.producerAccountId;
        const demoBucketKms = new kms.Key(this, 'ConsumerBucketKmsKey', {
            enabled: true,
            enableKeyRotation: true,
            alias: 'alias/ConsumerBucketKmsKey',
        });
        demoBucketKms.addToResourcePolicy(new iam.PolicyStatement({
            sid: 'Give the Firehose IAM role from the producer account access over this KMS key',
            effect: iam.Effect.ALLOW,
            principals: [new iam.AccountPrincipal(producerAccountId)],
            actions: ['kms:*'],
            resources: ['*'],
            conditions: {
                ['ArnEquals']: {
                    'aws:PrincipalArn': `arn:aws:iam::${props.producerAccountId}:role/${props.producerFirehoseRoleName}`
                }
            }
        }));
        const demoBucket = new s3.Bucket(this, 'ConsumerBucket', {
            bucketName: props.datalakeBucketName,
            blockPublicAccess: {
                blockPublicAcls: true,
                blockPublicPolicy: true,
                ignorePublicAcls: true,
                restrictPublicBuckets: true
            },
            accessControl: s3.BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            encryptionKey: demoBucketKms
        });
        demoBucket.addToResourcePolicy(new iam.PolicyStatement({
            actions: ['s3:DeleteObject', 's3:DeleteObjectTagging', 's3:GetObject', 's3:GetObjectTagging', 's3:ListBucket', 's3:PutObject', 's3:PutObjectTagging', 's3:PutObjectAcl'],
            resources: [demoBucket.bucketArn, `${demoBucket.bucketArn}/*`],
            conditions: {
                ['ArnEquals']: {
                    'aws:PrincipalArn': `arn:aws:iam::${producerAccountId}:role/${props.producerFirehoseRoleName}`
                }
            },
            principals: [new iam.AccountPrincipal(producerAccountId)]
        }));
        const ddbGlueJobRole = this.createGlueJobRole(props.producerDdbReadRoleName, producerAccountId, demoBucket);
        const glueJob = this.createGlueJob(ddbGlueJobRole, props.producerDdbTableName, producerAccountId, props.producerDdbReadRoleName, demoBucket);


        new cdk.CfnOutput(this, 'ConsumerBucketArn', { value: demoBucket.bucketArn, description: 'The ARN of the consumer bucket' });
        new cdk.CfnOutput(this, 'ConsumerBucketKmsKeyArn', { value: demoBucketKms.keyArn, description: 'The ARN of the consumer bucket KMS key' });
        new cdk.CfnOutput(this, 'DdbGlueJobRoleArn', { value: ddbGlueJobRole.roleArn, description: 'The ARN of the Glue job role' });
        new cdk.CfnOutput(this, 'DdbGlueJobRoleId', { value: ddbGlueJobRole.roleId, description: 'The role ID of the Glue job role' });
    }

    private createGlueJob = (glueJobRole: iam.IRole, producerDdbTableName: string, producerAccountId: string, producerDdbReadRoleName: string, dataLakeBucket: s3.IBucket): glue.CfnJob => {
        const workType = 'Standard';
        const numWorkers = 1;
        return new glue.CfnJob(this, 'DdbGlueJob', {
            role: glueJobRole.roleArn,
            workerType: workType,
            glueVersion: '4.0',
            name: 'ddb-cross-account-full-load-glue-job',
            numberOfWorkers: numWorkers,
            defaultArguments: {
                '--PRODUCER_DYNAMODB_NAME': producerDdbTableName,
                '--PRODUCER_ACCOUNT_ID': producerAccountId,
                '--PRODUCER_DYNAMODB_READ_ROLE_NAME': producerDdbReadRoleName,
                '--PRODUCER_REGION': cdk.Aws.REGION,
                '--S3_BUCKET_NAME': dataLakeBucket.bucketName,
                '--S3_PREFIX': this.fixedS3Prefix,
                '--WORKER_TYPE': workType,
                '--NUM_WORKERS': numWorkers
            },
            command: {
                name: 'glueetl',
                pythonVersion: '3',
                scriptLocation: 'glue_jobs/ddb_full_load.py'
            },
            executionProperty: {
                maxConcurrentRuns: 1
            }
        })
    }

    private createGlueJobRole = (producerDdbReadRoleName: string, producerAccountId: string, dataLakeBucket: s3.IBucket): iam.Role => {
        return new iam.Role(this, 'GlueJobRole', {
            roleName: 'ddb-cross-account-full-load-glue-job-role',
            description: 'An IAM role to allow the Glue job to read from DynamoDB cross account and write to S3.',
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
            ],
            inlinePolicies: {
                ['CrossAccountAssumeRole']: new iam.PolicyDocument({
                    assignSids: true,
                    statements: [
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: ['sts:AssumeRole'],
                            resources: [`arn:aws:iam::${producerAccountId}:role/${producerDdbReadRoleName}`]
                        })
                    ]
                }),
                ['DataLakeReadWrite']: new iam.PolicyDocument({
                    assignSids: true,
                    statements: [new iam.PolicyStatement({
                        effect: iam.Effect.ALLOW,
                        actions: [
                            's3:GetObject',
                            's3:PutObject',
                            's3:DeleteObject',
                            's3:GetObject'
                        ],
                        resources: [
                            `${dataLakeBucket.bucketArn}/${this.fixedS3Prefix}/*`,
                        ]
                    })]
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
                                dataLakeBucket.encryptionKey!.keyArn
                            ]
                        })
                    ]
                }),
            }
        })
    }
}