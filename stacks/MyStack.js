import * as sst from "@serverless-stack/resources";
import * as sns from "aws-cdk-lib/aws-sns";
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import {Fn, Duration} from "aws-cdk-lib";
import {PythonLayerVersion} from '@aws-cdk/aws-lambda-python-alpha';
import {CfnEventSourceMapping, Runtime, StartingPosition, LayerVersion} from "aws-cdk-lib/aws-lambda";
import * as path from "path";
import {SnsAction} from "aws-cdk-lib/aws-cloudwatch-actions";
import {TreatMissingData} from "aws-cdk-lib/aws-cloudwatch";
import {PolicyStatement, Effect} from "aws-cdk-lib/aws-iam";

export default class MyStack extends sst.Stack {
    constructor(scope, id, props) {
        super(scope, id, props);

        // Defining downstream, error topic arn names
        const destTopicArn = Fn.importValue('order-received-event-sns-topic-arn');
        const recursiveTopicArn = Fn.importValue('order-recursive-event-sns-topic-arn');
        const errorTopicArn = Fn.importValue('lambda-execution-error-event-sns-topic-arn');

        const destTopic = new sst.Topic(this, "OrderReceivedEventTopic", {
            snsTopic: sns.Topic.fromTopicArn(this, "OrderReceivedEventSNS", destTopicArn),
        });

        const recursiveTopic = new sst.Topic(this, 'OrderRecursiveEventTopic', {
            snsTopic: sns.Topic.fromTopicArn(this, 'OrderRecursiveEventSNS', recursiveTopicArn),
        });

        const errorsTopic = sns.Topic.fromTopicArn(this, 'LambdaExecutionErrorSNSTopicARN', errorTopicArn);

        const storeOrderDataIngestionTable = dynamodb.Table.fromTableAttributes(this, 'StoreOrderDataIngestionFlagsTable', {
            tableName: Fn.importValue('store-order-data-ingestion-flags-dynamodb-table-arn'),
            tableStreamArn: Fn.importValue('store-order-data-ingestion-flags-dynamodb-table-stream-arn')
        });

        const orderProcessingTable = dynamodb.Table.fromTableName(this, 'OrderProcessingStatisticsTable', Fn.importValue('orders-ready-for-processing-dynamodb-table-arn'));

        const storeCredentialsTable = dynamodb.Table.fromTableName(this, 'StoreCredentialsTable', Fn.importValue('store-credentials-simple-table-arn'));

        const orderFailedProcessingTable = dynamodb.Table.fromTableName(this, 'OrderFailedProcessingTable', Fn.importValue('orders-failed-processing-dynamodb-table-arn'));

                // AWS public layer for AWS Distro for OpenTelemetry Collector (ADOT Collector)
        const layer_adot_arn = `arn:aws:lambda:${sst.Stack.of(this).region}:901920570463:layer:aws-otel-python-amd64-ver-1-11-1:1`
        const layer_adot = LayerVersion.fromLayerVersionArn(this, 'OpenTelemetryLayer', layer_adot_arn)



        // Defining function layer(heavy code) for lambda function
        const functionLayer = new PythonLayerVersion(this, 'lambda-recursive-order-ingestion-lambda-layer', {
            entry: path.join(__dirname, '..', '..', 'layer'), compatibleRuntimes: [Runtime.PYTHON_3_8]
        });

        // Define recursive order ingestion lambda function
        const recursiveOrderIngestionLambda = new sst.Function(this, 'lambda-recursive-order-ingestion', {
            functionName: 'lambda-recursive-order-ingestion',
            handler: 'app.lambda_handler',
            layers: [functionLayer, layer_adot],
            environment: {
                API_SECRETS_TABLE: storeCredentialsTable.tableName,
                ORDER_FLAGS_TABLE: storeOrderDataIngestionTable.tableName,
                ORDER_PROCESS_TABLE: orderProcessingTable.tableName,
                ORDER_FAILED_TABLE: orderFailedProcessingTable.tableName,
                BATCH_UPDATE_SIZE: "50",
                REGION_NAME: scope.region,
                ORDER_RECEIVED_TOPIC: destTopicArn,
                RECURSIVE_PROCESSING_TOPIC: recursiveTopicArn,
                AWS_LAMBDA_EXEC_WRAPPER: "/opt/otel-instrument", // for OpenTelemetry
            },
            permission: ['secretsmanager:*', "dynamodb:*", 'sns:*', "xray:*"]
        });

        // Add iam policy for dynamodb actions
        recursiveOrderIngestionLambda.addToRolePolicy(new PolicyStatement({
            effect: Effect.ALLOW,
            actions: ["dynamodb:*"],
            resources: [`arn:aws:dynamodb:${sst.Stack.of(this).region}:${sst.Stack.of(this).account}:table/*`]
        }));

        const cfnEventSourceMapping = new CfnEventSourceMapping(this, 'storeOrderDataIngestionFlagsTableMapping', {
            startingPosition: StartingPosition.TRIM_HORIZON,
            batchSize: 1,
            maximumBatchingWindowInSeconds: 1,
            eventSourceArn: storeOrderDataIngestionTable.tableStreamArn,
            functionName: recursiveOrderIngestionLambda.functionName
        });
        cfnEventSourceMapping.addPropertyOverride('FilterCriteria', {
            Filters: [{
                Pattern: JSON.stringify(({
                    eventName: ['INSERT']
                }))
            }]
        });

        // Attaching permissions to the lambda function to access to the topic
        recursiveOrderIngestionLambda.attachPermissions([destTopic, recursiveTopic]);

        // Adding alarms to the stack
        recursiveOrderIngestionLambda.metricErrors({
            period: Duration.minutes(1)
        }).createAlarm(this, 'lambda-recursive-order-ingestion-errors-alarm', {
            threshold: 1,
            actionsEnabled: true,
            treatMissingData: TreatMissingData.NOT_BREACHING,
            evaluationPeriods: 1,
            alarmDescription: 'Alarm if the SUM of Lambda errors is greater than or equal to the threshold (1) for 1 evaluation period'
        }).addAlarmAction(new SnsAction(errorsTopic));

        recursiveOrderIngestionLambda.metricThrottles({
            period: Duration.minutes(1)
        }).createAlarm(this, 'lambda-recursive-order-ingestion', {
            threshold: 1,
            actionsEnabled: true,
            treatMissingData: TreatMissingData.NOT_BREACHING,
            evaluationPeriods: 1,
            alarmDescription: 'Alarm if the SUM of Lambda throttles is greater than or equal to the threshold (1) for 1 evaluation period'
        }).addAlarmAction(new SnsAction(errorsTopic));

        recursiveTopic.addSubscribers(this, [recursiveOrderIngestionLambda]);

        this.addOutputs({
            FunctionName: {
                value: recursiveOrderIngestionLambda.functionName,
                exportName: 'lambda-recursive-order-ingestion-function-name'
            }
        });
    }
}
