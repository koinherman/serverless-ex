import logging
import shopify
import boto3
import json
import os
# from aws_xray_sdk.core import patch_all
# from aws_xray_sdk.core import xray_recorder
from utils import rate_limit, build_credentials, resp
from boto3.dynamodb.conditions import Attr

from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, SpanKind, TraceFlags, Link
tracer = trace.get_tracer(__name__)  # Get a tracer for the current module from the Global Tracer Provider

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger = logging.LoggerAdapter(logger, {})
client = boto3.client('sns')
# patch_all()


# @xray_recorder.capture('lambda_handler')
def lambda_handler(event, context):
    json_dumps = json.dumps(event)
    logger.info("Processing Event -> " + json_dumps)
    for record in event['Records']:
        logger.warning('Record received: %s', record)
        batch_size = int(os.environ['BATCH_UPDATE_SIZE'])
        shop_url = get_shop_url_from_event(record)
        logger.warning('Start Processing for Shop %s and batch Size is %s', shop_url, batch_size)
        process_orders_batch(
            shop_url=shop_url,
            limit=batch_size
        )
        if not get_order_items(shop_url, 1):
            logger.warning('Stop Processing for Shop: %s', shop_url)
            stop_processing(shop_url)
            process_orders_batch(
                shop_url=shop_url,
                limit=batch_size
            )
            logger.warning('Deleted Flag for Shop: %s', shop_url)
        else:
            logger.warning('Continue Recursive Processing for Shop: %s', shop_url)
            send_sns_event_to_continue(shop_url)
            logger.warning('Continue Recursive Flow for Shop: %s', shop_url)

    logger.warning('Successfully finished function')
    return True


# @xray_recorder.capture('process_orders_batch')
def process_orders_batch(shop_url, limit):
    with tracer.start_as_current_span("Retrieving orders from shopify for one shop"):
        order_items = get_order_items(shop_url, limit)
        if not order_items:
            return False

        logger.warning(f'Ingesting following ids: {order_items.keys()}')

        ingested_order_ids = ingest_order_data(order_items, shop_url, limit)
        logger.warning('Successfully query from shopify and injested these orders: %s', ingested_order_ids)

        delete_ingested_order_ids([str(_) for _ in order_items.keys()])
        logger.warning('All orders deleted')
        return True


def get_order_items(shop_url, limit):
    dynamodb = boto3.client('dynamodb', region_name=os.environ['REGION_NAME'])
    items = dynamodb.query(
        TableName=os.environ['ORDER_PROCESS_TABLE'],
        IndexName='ShopUrl-index',
        KeyConditionExpression='shop_url = :shop_url',
        ExpressionAttributeValues={
            ':shop_url': {'S': shop_url},
        },
        Limit=limit,
    )
    if 'Items' in items:
        logger.warning('Orders returned from DynamoDb: %s', items)
        return {int(item['order_id']["S"]): item for item in items['Items']}
    else:
        logger.warning('No Orders returned from DynamoDb')
        return dict({})


# @xray_recorder.capture('ingest_order_data')
def ingest_order_data(order_items, shop_url, limit):
    ingested_ids = []
    shopify_orders_dict = {_obj.id: _obj for _obj in get_order_data(list(order_items.keys()), shop_url)}

    for order_id, order_item in order_items.items():
        ctx = None
        lambda_context = trace.get_current_span().get_span_context()
        tracing_info = order_item.get("opentelemetry_tracing")
        if tracing_info:
            logger.info(f"Found OpenTelemetry tracing context for order-{order_id}. Will connect to upstream xray servicemap.")
            ctx = trace.set_span_in_context(NonRecordingSpan(SpanContext(
                int(tracing_info["M"]["traceId"]["S"]), int(tracing_info["M"]["spanId"]["S"]), is_remote=True, trace_flags=TraceFlags(0x01))))
        with tracer.start_as_current_span(f'processing ingest for order{order_id}', context=ctx, kind=SpanKind.SERVER, links=[Link(lambda_context)]) as _span:
            shopify_order_obj = shopify_orders_dict.get(order_id)
            if shopify_order_obj:
                order_id = publish_order_receive(
                    dict(
                        shopId=shop_url,
                        limit=limit,
                        is_full_order=True,
                        **shopify_order_obj.to_dict()
                    )
                )
                ingested_ids.append(order_id)
            else:
                logger.warning(f"order_id-{order_id} not found in shopify, please check if it's archived")
                _span.set_attribute("order_status", "not found in shopify api, possibly archived")
    return ingested_ids


# @xray_recorder.capture('stop_processing')
def stop_processing(shop_url):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table(os.environ['ORDER_FLAGS_TABLE'])
    print(f'[Lambda]: Delete shop flag: {shop_url}')
    with table.batch_writer() as batch:
        batch.delete_item(
            Key={
                'shop_url': str(shop_url)
            }
        )


# @xray_recorder.capture('send_sns_event_to_continue')
def send_sns_event_to_continue(shop_url):
    message = {
        "shop_url": shop_url,
    }

    client.publish(
        TargetArn=os.environ["RECURSIVE_PROCESSING_TOPIC"],
        Message=json.dumps({'default': json.dumps(message)}),
        MessageStructure='json'
    )


# @xray_recorder.capture('delete_ingested_order_ids')
def delete_ingested_order_ids(order_ids):
    dynamodb = boto3.resource('dynamodb', region_name=os.environ['REGION_NAME'])
    table = dynamodb.Table(os.environ['ORDER_PROCESS_TABLE'])
    print(f'[Lambda]: Delete order ids: {order_ids}')
    if order_ids:
        _resp = table.scan(FilterExpression=Attr('order_id').is_in(order_ids))
        logger.warning(_resp)
        with table.batch_writer() as batch:
            for item in _resp['Items']:
                batch.delete_item(
                    Key={
                        'order_id': item['order_id']
                    }
                )


@rate_limit
# @xray_recorder.capture('get_order_data')
def get_order_data(order_ids, shop_url):
    credentials = build_credentials(shop_url)
    order_ids = list(map(str, order_ids))

    print(f'[Lambda]: Get order data: {order_ids} {shop_url}')

    try:
        with shopify.Session.temp(**credentials):
            orders = shopify.Order.find(
                ids=','.join(order_ids)
            )
    except Exception as e:
        logger.warning(f'Shopify Connection Issue: {e}')
        raise # must raise in case following logic break data
    return orders


# @xray_recorder.capture('push_order_receive')
def publish_order_receive(order_data):
    client = boto3.client('sns')
    logger.warning('Publish data: %s into topic %s', order_data, os.environ['ORDER_RECEIVED_TOPIC'])

    # inject tracing
    context = trace.get_current_span().get_span_context()
    order_data.update({
        'opentelemetry_tracing':{
            'traceId': context.trace_id,
            'spanId': context.span_id,
        }
    })

    response = client.publish(
        TargetArn=os.environ['ORDER_RECEIVED_TOPIC'],
        Message=json.dumps({'default': json.dumps(order_data)}),
        MessageStructure='json'
    )

    print(f'[Lambda-push-upstream-sns]: Response: {response}')

    return order_data['id']


# @xray_recorder.capture('get_shop_url_from_event')
def get_shop_url_from_event(current_record):
    if 'Sns' in current_record:
        js_sns_message = json.loads(current_record['Sns']['Message'])
        return js_sns_message['shop_url']
    else:
        return current_record['dynamodb']['Keys']['shop_url']['S']
