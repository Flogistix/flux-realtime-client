import argparse
import signal
import time
import requests
import base64
import json
from datetime import datetime

base_dev_url = 'https://dev-api.axil.ai/realtime'
base_prod_url = 'https://api.axil.ai/realtime'


# def handler(signum, frame):
#     res = input('Ctrl-c was pressed. Do you really want to exit? y/n ')
#     if res == 'y':
#         exit(99)


def get_auth0_token(client_id_, client_secret_):
    print(f'get_auth0_token :: starting')
    payload = {
        'client_id': client_id_,
        'client_secret': client_secret_,
        'audience': 'https://api.axil.ai',
        'grant_type': 'client_credentials'
    }
    headers = {'content-type': 'application/json'}

    req_resp = requests.post('https://axil.auth0.com/oauth/token',
                         headers=headers,
                         json=payload,
                         )
    if req_resp.status_code != requests.codes.ok:
        raise Exception('Error occurred getting Bearer token')
    print(f'get_auth0_token :: completed\n')
    return req_resp.json()


def get_stream_info(bearer_token_, base_url_,):
    print(f'get_stream_info :: starting')
    headers = {
        'content-type': 'application/json',
        'Authorization': f'Bearer {bearer_token_}'
    }
    req_resp = requests.get(base_url_, headers=headers,)
    if req_resp.status_code != requests.codes.ok:
        raise Exception('Error occurred getting stream information')
    print(f'get_stream_info :: completed\n')
    return req_resp.json()


def get_shard_iterator(bearer_token_, base_url_, shard_id_, shard_iterator_type_='LATEST'):
    print(f'get_shard_iterator :: starting')
    shard_iterator_url = f'{base_url_}/shard-iterator'
    headers = {
        'content-type': 'application/json',
        'Authorization': f'Bearer {bearer_token_}'
    }
    body = {
        'ShardId': shard_id_,
        'ShardIteratorType': shard_iterator_type_,
    }
    req_resp = requests.request(method='get', url=shard_iterator_url, headers=headers, json=body)
    if req_resp.status_code != requests.codes.ok:
        raise Exception('Error occurred getting stream information')
    print(f'get_shard_iterator :: completed\n')
    return req_resp.json()


def get_records(bearer_token_, base_url_, shard_iterator_, limit_):
    print(f'get_records :: starting')
    record_url = f'{base_url_}/records'
    headers = {
        'content-type': 'application/json',
        'Authorization': f'Bearer {bearer_token_}'
    }
    body = {
        'ShardIterator': shard_iterator_,
        'Limit': limit_,
    }
    req_resp = requests.request(method='get', url=record_url, headers=headers, json=body)
    if req_resp.status_code != requests.codes.ok:
        raise Exception('Error occurred getting stream information')
    recs = req_resp.json()
    rec_count = 0
    if 'Records' in recs and len(recs['Records']) > 0:
        rec_count = len(recs['Records'])
    print(f'get_records :: {rec_count} recs :: completed\n')
    return recs


if __name__ == '__main__':
    # signal.signal(signal.SIGINT, handler)
    print(f'main :: Hello World')

    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--environment',
                        help='The environment to be used',
                        choices=['dev', 'prod'],
                        default='dev',
                        )
    parser.add_argument('-c', '--clientId', help='Auth0 Client Id', required=True)
    parser.add_argument('-s', '--clientSecret', help='Auth0 Client Secret', required=True)
    parser.add_argument('-o', '--companyName', help='Company Name value provided by Flogistix', required=True)
    parser.add_argument('-l', '--limit', help='Record Limit to get on each call to the stream', default=100)
    parser.add_argument('-t', '--shardType',
                        help='The Shard Iterator type to use',
                        choices=['TRIM_HORIZON', 'LATEST'],
                        )
    args = parser.parse_args()

    token_obj = get_auth0_token(args.clientId, args.clientSecret)
    if args.environment == 'prod':
        base_url = base_prod_url
    else:
        base_url = base_dev_url

    company_url = f'{base_url}/{args.companyName.lower()}'
    bearer_token = token_obj['access_token']

    stream_info = get_stream_info(bearer_token, company_url)
    if 'StreamDescription' not in stream_info or 'Shards' not in stream_info['StreamDescription']:
        raise Exception('Stream info did not contain the correct information')
    iterators = {}
    for shards in stream_info['StreamDescription']['Shards']:
        shard_id = shards['ShardId']
        shard_iterator = get_shard_iterator(bearer_token, company_url, shard_id, args.shardType)
        if 'ShardIterator' in shard_iterator:
            iterators[shard_id] = shard_iterator['ShardIterator']

    while True:
        # Kinesis GetRecords has a limit of five transactions per second, per shard
        time.sleep(3)
        batch_data = []
        for iterator in iterators:
            shard_iter = iterators[iterator]
            resp = get_records(bearer_token, company_url, shard_iter, args.limit)
            if 'NextShardIterator' in resp:
                iterators[iterator] = resp['NextShardIterator']
            if 'Records' not in resp or len(resp['Records']) == 0:
                continue
            for rec in resp['Records']:
                data = base64.b64decode(rec['Data'])
                batch_data.append(json.loads(data))

        if len(batch_data) > 0:
            filename = f'batch-{datetime.now().isoformat()}.json'
            with open(filename, 'w') as outfile:
                json.dump(batch_data, outfile)

    print(f'main :: Goodbye World')
