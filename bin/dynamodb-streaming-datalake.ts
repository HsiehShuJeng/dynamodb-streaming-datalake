#!/usr/bin/env node
import { GetCallerIdentityCommand, STSClient } from "@aws-sdk/client-sts";
import { fromIni } from "@aws-sdk/credential-provider-ini";
import * as cdk from 'aws-cdk-lib';
import 'source-map-support/register';
import { ConsumerStack } from '../lib/consumer-components';
import { DynamodbStreamingDatalakeStack } from '../lib/dynamodb-streaming-datalake-stack';


const firehoseRoleName = 'firehose-dbstream-role';

const app = new cdk.App();
new DynamodbStreamingDatalakeStack(app, 'DynamodbStreamingDatalakeStack', {
    /* If you don't specify 'env', this stack will be environment-agnostic.
     * Account/Region-dependent features and context lookups will not work,
     * but a single synthesized template can be deployed anywhere. */

    /* Uncomment the next line to specialize this stack for the AWS Account
     * and Region that are implied by the current CLI configuration. */
    // env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },

    /* Uncomment the next line if you know exactly what Account and Region you
     * want to deploy the stack to. */
    env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
    stackName: 'dynamodb-streaming-datalake-demo',
    datalakeBucketName: `104-dev01-datalake`,
    datalakeBucketKeyAliasName: 'alias/DataLake',
    createNewKmsKey4Kinesis: true,
    firehoseRoleName: firehoseRoleName
    /* For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html */
});

const consumerApp = new cdk.App();
const sts = new STSClient({
    region: process.env.CDK_DEFAULT_REGION,
    credentials: fromIni({ profile: 'scott.hsieh' })
});

getAccountId().then(consumerAccountId => {
    console.log(`Producer Account ID: ${process.env.CDK_DEFAULT_ACCOUNT}`)
    console.log(`Consumer Account ID: ${consumerAccountId}`)
    new ConsumerStack(consumerApp, 'ConsumerStack', {
        env: { account: consumerAccountId, region: 'ap-northeast-1' },
        producerAccountId: process.env.CDK_DEFAULT_ACCOUNT!,
        producerFirehoseRoleName: firehoseRoleName
    });
});

async function getAccountId() {
    const command = new GetCallerIdentityCommand({});
    try {
        const data = await sts.send(command);
        return data.Account;
    } catch (err) {
        console.log("Error", err);
        throw err;
    }
}