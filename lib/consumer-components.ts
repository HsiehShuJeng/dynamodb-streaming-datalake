import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

interface ConsumerStackProps extends cdk.StackProps {
    readonly producerAccountId: string;
    readonly producerFirehoseRoleName: string;
}

export class ConsumerStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props: ConsumerStackProps) {
        super(scope, id, props);
        const demoBucketKms = new kms.Key(this, 'ConsumerBucketKmsKey', {
            enabled: true,
            enableKeyRotation: true,
            alias: 'alias/ConsumerBucketKmsKey',
        });
        demoBucketKms.addToResourcePolicy(new iam.PolicyStatement({
            sid: 'Give the Firehose IAM role from the producer account access over this KMS key',
            effect: iam.Effect.ALLOW,
            principals: [new iam.AccountPrincipal(props.producerAccountId)],
            actions: ['kms:*'],
            resources: ['*'],
            conditions: {
                ['ArnEquals']: {
                    'aws:PrincipalArn': `arn:aws:iam::${props.producerAccountId}:role/${props.producerFirehoseRoleName}`
                }
            }
        }));
        const demoBucket = new s3.Bucket(this, 'ConsumerBucket', {
            bucketName: `dynamodb-streaming-datalake-${cdk.Aws.ACCOUNT_ID}`,
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
                    'aws:PrincipalArn': `arn:aws:iam::${props.producerAccountId}:role/${props.producerFirehoseRoleName}`
                }
            },
            principals: [new iam.AccountPrincipal(props.producerAccountId)]
        }));

        new cdk.CfnOutput(this, 'ConsumerBucketArn', { value: demoBucket.bucketArn, description: 'The ARN of the consumer bucket' });
        new cdk.CfnOutput(this, 'ConsumerKmsKeyArn', { value: demoBucketKms.keyArn, description: 'The ARN of the consumer bucket KMS key' });
    }
}