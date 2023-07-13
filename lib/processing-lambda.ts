import * as cdk from 'aws-cdk-lib';
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { Construct } from "constructs";
import * as path from 'path';

interface JsonProcessorProps {
    kmsKinesisKeyArn: string;
}

export class JsonProcessor extends Construct {
    lambdaEntity: lambda.Function;
    constructor(
        scope: Construct,
        id: string,
        props: JsonProcessorProps
    ) {
        super(scope, id);
        const transformationLambdaRole = new iam.Role(this, 'TransformationFunctionRole', {
            roleName: 'Ddb-Delivery-Transformation-Role',
            assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
            description: 'A role to allow the transformation Lambda function to convert multi-line JSON into single-line JSON.',
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName(
                    'service-role/AWSLambdaBasicExecutionRole'
                ),
                iam.ManagedPolicy.fromAwsManagedPolicyName('AWSXRayDaemonWriteAccess'),
                iam.ManagedPolicy.fromAwsManagedPolicyName(
                    'service-role/AWSLambdaVPCAccessExecutionRole'
                ),
            ],
            inlinePolicies: {
                ['DefaultPolicy']: new iam.PolicyDocument({
                    assignSids: true,
                    statements: [
                        new iam.PolicyStatement({
                            actions: ['cloudwatch:PutMetricData'],
                            effect: iam.Effect.ALLOW,
                            resources: ['*']
                        }),
                        new iam.PolicyStatement({
                            actions: ['kms:Encrypt', 'kms:Decrypt', 'kms:ReEncrypt', 'kms:GenerateDataKey*'],
                            effect: iam.Effect.ALLOW,
                            resources: [props.kmsKinesisKeyArn]
                        })
                    ]
                })
            }
        });
        this.lambdaEntity = new lambda.Function(this, 'TransformationLambda', {
            functionName: 'ddb-delivery-transformation',
            runtime: lambda.Runtime.PYTHON_3_10,
            code: lambda.Code.fromAsset(path.join(
                __dirname, '../resources'
            )),
            timeout: cdk.Duration.seconds(300),
            handler: 'firehose_transformation.lambda_handler',
            role: transformationLambdaRole,
            architecture: lambda.Architecture.ARM_64,
        })
    }
}