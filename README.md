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

<ul>
<li>MySQL server and client (v5.5 or later).</li>
<li>Python programming language (v2.7 or later, but not the v3 branch), 
MySQLdb (v1.2.3 or later), lxml (v3.3.1-0 or later), beautifulsoup4 (v4.2.1 or later),
requests (v2.2.1 or later) and configparser (v3.3.0r2 or later).</li>
<li>R programming language and environment (v 2.15.0 or later).</li>
<li>Additional R libraries with extra data and functionalities (This list will be updated as new
functionalities are included in this toolkit):</li> 
<ul>
<li>RMySQL: Connect to MySQL databases from R.</li>
<li>Hmisc: Frank Harrell's miscelaneous functions (essential).</li>
<li>car: Companion library for "R Companion to Applied Regression", 2nd ed.</li>
<li>DAAG: Companion library for "Data Analysis and Graphics using R."</li>
<li>ineq: Calcualte inequality metrics and graphics.</li>
<li>ggplot2: A wonderful library to create appealing graphics in R.</li>
<li>eha: Library for event history and survival analysis.</li>
<li>zoo: Excellent library to handle time series data. </li>
<ul>
