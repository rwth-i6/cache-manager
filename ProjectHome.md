CacheManager is a simple but effective solution for data distribution in cluster computer environments without a sophisticated storage system. It is a non-intrusive solution consisting of Python tools.


A problem often occuring in distributed computing system, lacking sophisticated (and expensive) storage systems or distributed file systems, is the frequent overloading of  file servers due to massively parallel file access. The overcharge of file servers is usually avoided by keeping copies of the files on local hard disks of the compute nodes.

CacheManager simplifies the caching and reduces the load of the file servers. It uses a central server instance which manages all caching activities of all compute jobs, such that the number of parallel IO-activities is limited to a reasonable amount. It requires neither modifications of the operating system nor software changes of the jobs running in the cluster.

The usage is really simple - you call the tool with a filename as argument and retrieve the path of the local copy. Anything else is handled by the caching tool:
  * copying the file to the local disk
  * limiting the parallel access to the file server(s)
  * keeping track of existing local copies on other nodes
  * controlling the usage of the local disks


CacheManager has been developed by the [Human Language Technology and Pattern Recognition Group](http://www.hltpr.rwth-aachen.de) at [RWTH Aachen University](http://www.rwth-aachen.de), Germany. It has been used successfully for several years in a cluster computer system with more than 1000 parallel running jobs.


A detailed description can be found in the Wiki.