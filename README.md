storm-filterregex-bolt
======================
This bolt receives a byte array and search for regex patterns configured in propery file (see the [tcp topology](https://github.com/keedio/Storm-TCP-Topology)); if the message contains a whitelisted pattern then it's emitted to next bolt, other case the message is discarded. 

```
...
# OPTIONAL PROPERTIES

# Filter messages rules, regexp expression are used
# If allow is setted only the messages matching the regexp will be sent to host:port configured via TCP
#filter.bolt.allow=.*||22.9.43.17.*
# If deny is setted the messages matching the regexp will be discarded
#filter.bolt.deny=
...
```

Compilation
===========
Use maven
````
mvn clean package
```
