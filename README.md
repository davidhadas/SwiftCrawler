Swift Crawler
-------------

* WARNING: THIS IS WORK IN PROGRESS AND IS PUBLISHED TO ALLOW *
* ADDITIONAL COMMUNITY DEVELOPMENT AND TESTING                *

A crawler that walks swift devices and activates pluggable 'background' services
such as auditors, synchronizers, replicators, updaters etc.

The Swift Crawler offers a uniform framework for handling eventual consistency,
detecting errors, performing cleanups etc. by replacing some or all of the
individual Swift Daemons used today. 

Advantages of using the Crawler
-------------------------------
1) Performance improvements No. 1
   A single walk on the datadirs results in less IO and more efficient usage 
   of server inode caching. 
   if we have N daemons walking the same tree, by the time the second daemon
   reaches some inode, it may already be cached out. 
   Also, walking N times takes more resources than walking once. 

2) Performance improvements No. 2
   A single walk can also improve performance by performing a single execution
   where multiple executions previously existed. For example consider daemons
   handling containers - if we can open the container DB just once we can gain
   CPU cycles and IO over opening the same DB N times for N 'background'
   services (as done today by the N separate daemons).
 
3) Pluggable
   Adding a new service no longer require writing a new daemon, instead the
   pluggable service defines where it would like to hook into the crawler and
   the crawler will call the service get called for each item at the right
   time.

4) Maintainability via Code Reuse
   Today there are many daemons doing the same work in different ways, 
   this means more bugs and more effort to find and fix bugs or extend the
   system.

5) Tuning the system
   Today we have many daemons taking resources individually and without
   mechanisms to tune them. 
   The Swift Crawler framework offers a unique opportunity to gain control.
   Each execution is measured allowing automated or manual tuning of the
   system. A simple automated tuning mechanism is offered from day 1.   

6) Taking Control
   The Swift Crawler targets offering more control to the deployer by
   supporting Admin defined cycle time for the crawl. 
   The Admin can define the services that would need to complete during 
   each cycle. Services can also be defined to complete within any integer
   number of cycles. 
   
   The Admin than defines when cycles are started such that he may avoid
   using system resources for the background services during peak time.
   The Crawler ensures that the system resources are balanced and equally
   distributed along the cycle time.  


Background
----------
Daemons are used in Swift to achieve eventual consistency, detect errors, and
cleanup old data. Daemons needs to balance between performance and consistency
- the more resources they use, the higher the consistency. Different use cases
have different performance and consistency criteria and require different
background service tuning.

Swift uses a set of independent daemons for auditing, replication, updating
objects, accounts and containers as well as other special daemons - e.g. 
container sync daemons. Each daemon walk the directory tree independently
while performing its individual task resulting in cache inefficiencies and
and increase IO demand due to multiple non-synchronized directory walks.
The different daemons also offers little in the sense of code reuse. 
 
The Crawler
-----------
A Swift Crawler enables:
1. A single directory walk in which all work will be done
   - improved performance, reduced i/o load
2. Pluggable services allowing calls to methods in the respective auditing,
   replication, updating and other services as well as allowing vendor
   specific extensions.
3. Central per node DB for maintaining runtime parameters extendable to
   allow crawler thread tuning (out side of the scope of this work) and
   stats (allowing monitoring)
 
Design:
-------
A Crawler Class running as a single process reading current configuration and
loading pluggable services, than walking the Swift Storage Node devices and
instantiating a thread per device per datadir
(e.g. 'accounts' 'containers', 'objects').  

Each thread will run a single instance of a DataCrawler Class. 
It is possible to later run the Crawler Class using Linux cron or other
scheduling mechanism and ensuring that cycles would complete prior to 
use case peak times.

                  ------------- 
                  |  Crawler  |
                  -------------   
                    /      \
                   /        \ 
                  /          \ 
      ---------------      -------------
      | DataCrawler | ---- | CrawlerDB |
      ---------------      -------------
         |
         |--Service1
         |
         |--Service2
         |
         |--Service3
             

A DataCrawler Class runs as a single thread crawling a specified datadir and
calling the pluggable services. The DataCrawler is also responsible for:
- Pacing the work using a slowdown mechanism to balance the load along 
  the cycle
- Supporting a multi-cycle service by dividing the services to sets and 
  walking the right set each cycle. 
- Measuring the total time used , total slowdown used and stats
- Updating a Crawler DB Class about the outcomes of the execution
- Optionally automating the slowdown mechanism based on previous runs
 
A Crawler DB Class is used to store the stats and runtime parameters of the
different Data Crawler instances - this class is implemented using sqlite3.

A configuration file is used to define the pluggable services.

Installation and Usage
----------------------
In this first take:
1. mkdir /var/swift and make it owned by your swift uid
2. copy the example ./etc/swift-crawler.conf to /etc/swift/ 
   and make it owned by your swift uid
3. change /etc/swift/swift-crawler.conf to fit your system
4. run `python crawel/crawler.py` whenever you want a cycle to start
5. ping me (davidhadas) on #openstack-swift if I missed anything



