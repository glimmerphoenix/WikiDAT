WikiDAT
=======

### Wikipedia Data Analysis Toolkit

+ **Author**: Felipe Ortega.
+ **Contributors**: Carlos Martínez, Efrayim D Zitron, Aaron Halfaker.
+ **License**: [GPLv3](http://www.gnu.org/licenses/gpl.txt).

The aim of WikiDAT is to create an extensible toolkit for Wikipedia data 
analysis, using Python and R.

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

### Required dependencies

For a complete list of hardware and software requirements, please check the 
[requirements.md](https://github.com/glimmerphoenix/WikiDAT/blob/master/requirements.md)
file.
