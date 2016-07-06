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

The following external dependencies are required to run all examples included 
in WikiDAT:

* **MySQL** server and client (v5.5 or later) or **MariaDB** server and client 
(v5.5 or later; v10.0 or later).
* **Python** programming language (v3.4.2 or later; **Python 2 is not 
supported anymore**).
* **R** programming language and environment (v3.2.1 or later).
* **Redis** (server and client). Packages `redis-server` and `redis-tools` in 
Debian and Ubuntu.
* Software for file compression/decompression: zip, 7-zip, bz2, etc. In 
particular, 7-zip is mandatory for large dump data files (package `p7zip-full` 
in Debian and Ubuntu).
* In Debian and Ubuntu, it is advisable to install the `dateutil` and `pyzmq`
Python modules from packages: `python-zmq` and `python-dateutil`, respectively.

It is very easy to create an isolated Python execution environment with Python
3 and the required Python packages to test WikiDAT, either using `virtualenv` 
or creating a new Python 3.5 environment using the 
[Anaconda Python distribution] (https://www.continuum.io/downloads):

* http://conda.pydata.org/docs/test-drive.html#managing-envs

#### Python packages
**Included in Python 3.5**
* configparser (v3.3.0r2 or later).
* ipaddress (v1.0.7 or later).

**Included in Anaconda distribution (install them with `pip install` if you do not use Anaconda)**
* lxml (v3.3.1-0 or later).
* beautifulsoup4 (v4.2.1 or later).
* pyzmq (v14.3.0 or later, see above).
* dateutils (v2.2 or later, see above).
* requests (v2.2.1 or later).
* redis (v2.10.3 or later).

**To be installed (using `conda install` in Anaconda or `pip install` in regular Python)**
* PyMySQL (v0.6.7 or later).
* ujson (v1.3.0 or later).

#### R packages (CRAN)
* RMySQL: Connect to MySQL databases from R.
* Hmisc: Frank Harrell's miscelaneous functions (essential).
* car: Companion library for "R Companion to Applied Regression", 2nd ed.
* DAAG: Companion library for "Data Analysis and Graphics using R."
* ineq: Calcualte inequality metrics and graphics.
* ggplot2: A wonderful library to create appealing graphics in R.
* eha: Library for event history and survival analysis.
* zoo: Excellent library to handle time series data. 
