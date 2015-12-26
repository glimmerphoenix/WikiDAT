WikiDAT
=======

### Wikipedia Data Analysis Toolkit

+ **Author**: Felipe Ortega.
+ **Contributors**: Carlos Martínez, Efrayim D Zitron, Aaron Halfaker.
+ **License**: [GPLv3](http://www.gnu.org/licenses/gpl.txt).
+ **Python version**: 3.4.2

The aim of WikiDAT is to create an extensible toolkit for Wikipedia data 
analysis, using Python and R.

**IMPORTANT**: WikiDAT must be executed in **Python 3** (v3.4.2 or 
later) and **R 3.2.1** (or later) to work correctly. Despite previous versions
of this toolkit were implemented on Python 2, that platform is not supported
anymore.

Several tools are included to automate the extraction and preparation of 
Wikipedia data from different sources. Their execution can be parallelized in 
multi-core computing environments, and they are highly customizable with a 
single configuration file.

Different case studies illustrate how to analyze and visualize data from 
Wikipedia in any language. Outcomes are stored in subdirectories `results`, 
`figs` or `traces`, inside the main directory for each case. More cases will 
be included progressively, covering typical examples of quantitative analyses 
that can be undertaken with Wikipedia data.

Currently, WikiDAT is compatible with either MySQL or MariaDB for local
data storage. Support for PostgreSQL will be available soon (code is being 
ported). Additional support for unstructured data with MongoDB is also 
planned.

### Ongoing changes

The toolkit has been migrated to Python 3 (v 3.4.2 or later). The codebase 
should still be thoroughly tested to ensure proper functioning and absence 
of bugs. Likewise, all documentation is undergoing a complete update to 
reflect the new changes for execution under Python 3.

### Required dependencies

For a complete list of hardware and software requirements, please check the 
[requirements.md](https://github.com/glimmerphoenix/WikiDAT/blob/master/requirements.md)
file.
