#storm-filterregex-bolt
##Description

This bolt receives a byte array and search for regex patterns configured in propery file. 

If the message contains a whitelisted pattern, then it's emitted to the default stream and the "success" bolt stream.
Other case the message is sent to "failure" bolt stream (if the stream failure is not connected in the topology, the message will be discarded)

If whitelist isn't configured, blacklist can be used to discard all messages that contains a blacklisted pattern

## Property file configuration

|property|default|description
|--------|------------|-------------|
|filter.bolt.whitelist.1||Whitelist pattern 1|
|filter.bolt.whitelist.2||Whitelist pattern 2|
|filter.bolt.blacklist.1||Blacklist pattern 1|
|filter.bolt.blacklist.2||Blacklist pattern 2|
|filter.bolt.enriched|false|true if expected input messages will be in Keedio enriched format|

1 to n regex patterns can be specified.

## Using Keedio Enriched format

Message 1:
```json
{
 "extraData":{"hostname": "localhost", "domain": "localdomain", "date": "2015-04-23", "time": "07:16:08"},
 "message": "The original body string"
}
```
Message 2:
```json
{
 "extraData":{"hostname": "localhost", "domain": "localdomain", "date": "2015-04-23", "time": "07:16:08"},
 "message": "Not match body string"
}
```

With config
```conf
filter.bolt.enriched = true
filter.bolt.whitelist.1 = original 
```
The message 1 will be sent to "success" and "default" streams and message 2 will be sent to "failure" stream 

## Compilation
Use maven
````
mvn clean package
```
