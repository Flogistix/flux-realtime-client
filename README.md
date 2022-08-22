<p align="center">
<img src="./assets/images/FluxRealtime.png" alt="Flux Realtime"/>
</p>
Flux Realtime is a data delivery service that allows Flogistix 
to deliver all the data we collect from our Flogistix Compressors
to our customers.  

The Flux Realtime API is secured by Auth0 and the client must authorize
to Auth0 and retrieve a Bearer Token that is to be used on API calls
to retrieve records.

The API is backed and secured by an AWS Kinesis stream. Therefore, 
documentation can be reviewed for a further understanding of Kinesis
streams.  
https://docs.aws.amazon.com/kinesis/latest/APIReference/API_Operations.html

The base url of the Flux Realtime is as follows:    
DEV - `https://dev-api.axil.ai/realtime/`    
PRD - `https://api.axil.ai/realtime/`  

Additionally, the customer name will be added on to distinguish each
customer.  
Ex. `https://dev-api.axil.ai/realtime/company`

### Get Stream Info  
HTTP Verb: `GET`  
Url: `https://dev-api.axil.ai/realtime/company`  
Body: None  
Response:
```
{
	"StreamDescription": {
		"EncryptionType": "KMS",
		"EnhancedMonitoring": [
			{
				"ShardLevelMetrics": []
			}
		],
		"HasMoreShards": false,
		"KeyId": "alias/aws/kinesis",
		"RetentionPeriodHours": 24,
		"Shards": [
			{
				"HashKeyRange": {
					"EndingHashKey": "<val>",
					"StartingHashKey": "<val>"
				},
				"SequenceNumberRange": {
					"StartingSequenceNumber": "<val>"
				},
				"ShardId": "shardId-000000000000"
			}
		],
		"StreamARN": "arn:aws:kinesis:<region>:<account>:stream/flux-realtime-<company>",
		"StreamCreationTimestamp": <val>,
		"StreamModeDetails": {
			"StreamMode": "PROVISIONED"
		},
		"StreamName": "<stream-name>",
		"StreamStatus": "ACTIVE"
	}
}
```
### Get Shard Iterator  
HTTP Verb: `GET`  
Url: `https://dev-api.axil.ai/realtime/company/shard-iterator`  
Body:  
```
{
	"ShardId": "<shard-id>",
	"ShardIteratorType": "<shard-iterator-type One of: TRIM_HORIZON, LATEST>"
}
```
Response:  
```
{
	"ShardIterator": "<shard-iterator>"
}
```

### Get Records  
HTTP Verb: `GET`  
Url: `https://dev-api.axil.ai/realtime/company/records`  
Body:  
```
{
	"ShardIterator": "<shard-iterator>",
	"Limit": <count of records up to 10,000>
}
```
Response:  
```
{
	"MillisBehindLatest": 0,
	"NextShardIterator": "<next shard iterator>",
	"Records": [
		{
			"ApproximateArrivalTimestamp": <val>,
			"Data": "<data>",
			"EncryptionType": "KMS",
			"PartitionKey": "<pk>",
			"SequenceNumber": "<sequence number>"
		},
	]
}
```

There is a throttling limit of 100 requests per minute. Since each 
call can receive up 10,000 records. Calls above this limit will be rate
limited and/or additional charges may apply to raise the limit.

Below is an example payload that you could receive.

```
{
	"epochMs": 1652128223000,
	"registerId": 416,
	"val": 2,
	"rtms": 1652128223567,
	"pk": "1_333_2222_1111_1754_889_21_108",
	"tagId": 889,
	"label": "Valve Loaded Position",
	"tagName": "register_tag__loading_valve_#4_status",
	"orgId": 333,
	"siteId": 2222,
	"assetId": 1111
}
```

The `assetId` here can be used to get asset information from our Flux Enterprise Objects API. Located here [Flux Enterprise Object Client Documentation](https://github.com/Flogistix/flux-enterprise-object-client)
