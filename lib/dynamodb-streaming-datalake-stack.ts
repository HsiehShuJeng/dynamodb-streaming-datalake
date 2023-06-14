import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

interface DynamodbStreamingDatalakeStackProps extends cdk.StackProps {
    datalakeBucketName: string,
    datalakeBucketKeyAliasName: string
}

export class DynamodbStreamingDatalakeStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props: DynamodbStreamingDatalakeStackProps) {
        super(scope, id, props);

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

        // The code that defines your stack goes here

        // example resource
        // const queue = new sqs.Queue(this, 'DynamodbStreamingDatalakeQueue', {
        //   visibilityTimeout: cdk.Duration.seconds(300)
        // });
        new cdk.CfnOutput(this, 'DatalakeBucketArn', { value: datalakeBucket.bucketArn, description: 'Datalake Bucket ARN' });
        new cdk.CfnOutput(this, 'DdbStreamArn', { value: ddbStream.streamArn, description: 'The ARN of the Kinesis Stream for DynamoDB' });
        new cdk.CfnOutput(this, 'DataLakeBucketKeyArn', { value: datalakeBucketKey.keyArn });
        new cdk.CfnOutput(this, 'DdbTableArn', { value: exampleDdbTable.tableArn, description: 'The ARN of the DynamoDB table' })
    }
}
