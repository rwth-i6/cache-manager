# Introduction #

By default, CacheManager server will write usage statistics to a log file. The logging interval can be set by the [configuration](Configuration.md) parameter `STAT_INTERVAL`. The gathered statistics can be used to evaluate the effectiveness of a particular CacheManager setup.

# Example #

An entry in your log file may look like this:

```
LOG: statistics at 2012-06-26 11:24:40
     requests:       3101136
     active threads: 13
     server copy:    532969 = 49.27
     node copy:      548844 = 50.73
     waits:          281375
     aborted:        283
     files:          277343
     locations:      610013 = 2.20 per file
```

The values are accumulated since the server startup.

It may be useful to compare the number of file copies obtained directly from a server (`server copy`) with the number of file copies obtained from the local disks of your compute nodes (`node copy`). In this example, approximately every second file could be copied from a node, resulting in a relief factor of about 2.