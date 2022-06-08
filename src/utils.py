import os
import time

import boto3
from shopify.base import ShopifyConnection
import pyactiveresource.connection
from functools import wraps


def resp(body_data):
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': body_data,
        "isBase64Encoded": False
    }


def rate_limit(func):
    @wraps(func)
    def decorated(*args, **kwargs):
        patch_shopify_with_limits()
        return func(*args, **kwargs)

    return decorated


def patch_shopify_with_limits():
    func = ShopifyConnection._open

    def patched_open(self, *args, **kwargs):
        while True:
            try:
                return func(self, *args, **kwargs)

            except pyactiveresource.connection.ClientError as e:
                if e.response.code == 429:
                    retry_after = float(e.response.headers.get('Retry-After', 4))
                    print('Service exceeds Shopify API call limit, '
                          'will retry to send request in %s seconds' % retry_after)
                    time.sleep(retry_after)
                else:
                    raise e

    ShopifyConnection._open = patched_open


def build_credentials(shop_url):
    secret_token = _get_secret(shop_url)
    return dict(
        domain=shop_url,
        version='2021-10',
        token=secret_token
    )


def _get_secret(shop_url):
    dynamodb = boto3.resource('dynamodb', region_name=os.environ['REGION_NAME'])
    table = dynamodb.Table(os.environ['API_SECRETS_TABLE'])
    has_item = table.get_item(Key={'id': shop_url.split('://')[-1]})
    if 'Item' in has_item:
        return str(has_item['Item']['shopSecretKey'])
    else:
        return 'EmptyKey'
