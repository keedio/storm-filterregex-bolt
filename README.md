#storm-filterregex-bolt
##Description

This bolt receives a byte array and search for regex patterns configured in propery file (see the [tcp topology](https://github.com/keedio/Storm-TCP-Topology)); if the message contains a whitelisted pattern then it's emitted to next bolt, other case the message is discarded. 
If configured, you can get the patterns group indicated.


```
...
# OPTIONAL PROPERTIES

# Filter messages rules, regexp expression are used
# If allow is setted only the messages matching the regexp will be sent to host:port configured via TCP
<<<<<<< HEAD
filter.bolt.allow=.*||22.9.43.17.*
# If deny is setted the messages matching the regexp will be discarded
filter.bolt.deny=
conf.pattern1=(<date>[^\\s]+)\\s+(<time>[^\\s]+)\\s+
conf.pattern2=(<date>[^\\s]+)\\s+
group.separator=|
ganglia.report = no

# if Ganglia reporting is needed:
ganglia.report = yes
ganglia.host = localhost
ganglia.port = 5555
ganglia.ttl = 1
ganglia.UDPAddressingMode = (UNICAST | MULTICAST)
ganglia.seconds = 60
...
```

|property|mandatory|description
|--------|------------|-------------|
|filter.bolt.deny|false|Regex indicating match pattern of blacklisted messages|
|filter.bolt.allow|false|Regex indicating match pattern of whitelisted messages|
|conf.pattern1|false|Regex indicating group of patterns looked for in the message|
|conf.pattern2|false|Regex indicating group of patterns looked for in the message|
|group.separator|false|String used to separate the differents patterns|
|ganglia.report|true|String used to evaluate if Ganglia reporting is needed|
|ganglia.host|false|server's name allocating Ganglia|
|ganglia.port|false|server's port allocating Ganglia|
|ganglia.ttl|false|time to live|
|ganglia.UDPAddressingMode|false|mode for inet address|
|ganglia.seconds|false|duration report|

## Example
See [test classes](https://github.com/keedio/storm-filterregex-bolt/blob/feature/horizfilter/src/test/java/com/keedio/storm/FilterBoltTest.java) for more information

## Compilation
=======
#filter.bolt.allow=.*||22.9.43.17.*
# If deny is setted the messages matching the regexp will be discarded
#filter.bolt.deny=
...
```

Compilation
===========
>>>>>>> 11593a1d9113cf14fc3df229f3d950fd1bd94927
Use maven
````
mvn clean package
```
