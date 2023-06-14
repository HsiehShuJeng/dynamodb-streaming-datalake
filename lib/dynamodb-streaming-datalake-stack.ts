import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

interface DynamodbStreamingDatalakeStackProps extends cdk.StackProps {
    datalakeBucketName: string
}

export class DynamodbStreamingDatalakeStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: DynamodbStreamingDatalakeStackProps) {
        super(scope, id, props);

        const datalakeBucket = (props?.datalakeBucketName) ? s3.Bucket.fromBucketName(this, 'DataLakeBucket', props.datalakeBucketName) : new s3.Bucket(this, 'DatalakeBucket', {
            bucketName: `dynamodb-streaming-datalake-${cdk.Aws.ACCOUNT_ID}`
        })

        // The code that defines your stack goes here

        // example resource
        // const queue = new sqs.Queue(this, 'DynamodbStreamingDatalakeQueue', {
        //   visibilityTimeout: cdk.Duration.seconds(300)
        // });
        new cdk.CfnOutput(this, 'DatalakeBucketArn', { value: datalakeBucket.bucketArn, description: 'Datalake Bucket ARN' })
    }
}
