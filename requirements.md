Hardware requirements
---------------------
Minimum hardware requirements depend on the size of the Wikipedia language
version that you want to analyze. For small and mid-size languages, you can
even run the whole process in a modest laptop, with at least 4 GB of RAM and
50 GB of free space on disk.

For large Wikipedias, it is advisable to execute WikiDAT in a multi-core
system with as much RAM as possible (16 GB or more). Make sure that you 
configure your MySQL/MariaDB server to take advantage of this. Besides, 
execution time can be substantially reduced by using SSDs for data storage.

Software requirements
---------------------
### Operating Systems

The current *beta* version of WikiDAT has been tested in **Ubuntu** and 
**Debian** GNU/Linux platforms.

WikiDAT has also been tested in Windows 7 / 8 platforms. However, the
documentation does not currently provide installation and execution details
for that platform.

WikiDAT has not been tested on Mac platforms, so far.

### Software dependencies

The following dependencies are required to run all examples currently included 
in WikiDAT:

* **MySQL** server and client (v5.5 or later) or **MariaDB** server and client 
(v5.5 or later; v10.0 or later).
* **Python** programming language (v2.7 or later; **Python3 not supported**).
* **R** programming language and environment (v 2.15.0 or later).
* **Redis** (server and client). Packages `redis-server` and `redis-tools` in 
Debian and Ubuntu.
* Software for file compression/decompression: zip, 7-zip, bz2, etc. In 
particular, 7-zip is mandatory for large dump data files (package `p7zip-full` 
in Debian and Ubuntu).
* In Debian and Ubuntu, it is advisable to install the `dateutil` and `pyzmq`
Python modules from packages: `python-zmq` and `python-dateutil`, respectively.

#### Python packages
* MySQLdb (v1.2.3 or later).
* lxml (v3.3.1-0 or later).
* beautifulsoup4 (v4.2.1 or later).
* pyzmq (v14.3.0 or later, see above).
* dateutils (v2.2 or later, see above).
* requests (v2.2.1 or later).
* ujson (v1.3.0 or later).
* configparser (v3.3.0r2 or later).
* redis (v2.10.3 or later).
* ipaddress (v1.0.7 or later).

#### R packages 
* RMySQL: Connect to MySQL databases from R.
* Hmisc: Frank Harrell's miscelaneous functions (essential).
* car: Companion library for "R Companion to Applied Regression", 2nd ed.
* DAAG: Companion library for "Data Analysis and Graphics using R."
* ineq: Calcualte inequality metrics and graphics.
* ggplot2: A wonderful library to create appealing graphics in R.
* eha: Library for event history and survival analysis.
* zoo: Excellent library to handle time series data. 
