WikiDAT
=======

<b>Wikipedia Data Analysis Toolkit</b>

<b>Authors</b>: Felipe Ortega, Aaron Halfaker.</br>
<b>License</b>: GPLv3 (http://www.gnu.org/licenses/gpl.txt).

The aim of WikiDAT is to create an extensible toolkit for Wikipedia Data 
Analysis, based on MySQL, Python and R.

<p>Each module implements a different type of analysis, storing the output in 
subdirectories <i>results</i>, <i>figs</i> or <i>traces</i>, created in the 
module's directory. Module source code includes Python and R code to implement 
both the data preparation/cleaning and data analysis steps, including inline 
comments. An important goal is to illustrate different case examples of 
interesting analyses with Wikipedia data, following a didactic approach.</p>

<p>The long-term goal is to include more case examples progressively, in order 
to cover many of the usual examples of quantitative analyses that can be 
undertaken with Wikipedia data. In the future, this may also include the use 
of tools for distributed computing to support analysis of really huge data 
sets in high-resolution studies.</p>

<b>Required dependencies</b>
The following software dependencies are required to run all examples currently
included in WikiDAT:

<ul>MySQL server and client (v5.5 or later).</ul>
<ul>Python programming language (v2.7 or later, but not the v3 branch) and 
MySQLdb (v1.2.3)</ul>
<ul>R programming language and environment (v 2.15.0 or later).</ul>
<ul>Additional R libraries with extra data and functionalities: RMySQL, Hmisc, 
car, DAAG, ineq, ggplot2, eha. (This list will be updated as new 
functionalities are included in this toolkit).</ul>
