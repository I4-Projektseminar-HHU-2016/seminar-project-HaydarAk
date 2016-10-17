Computing Wikipedia's internal PageRank 
=============

Computing english Wikipedia’s internal PageRank

Features
--------

-   [x] SQL Dump Reading & Parsing

-   [x] Generate SQLite-Database with parsed Wiki-Articles & Link data

-   [ ] Computing PageRank

-   [ ] visualization of results

Getting Started
---------------

These instructions will get you a copy of the project up and running on your local machine.

 

### 1. Prerequisites

Python 3.5 with following libraries:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
pickle
glob 
sqlite3 
psutil
os
errno
time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 

### 2.1 Download Project:

 

-   **Download the project, using git:**

1.  Create a folder on your computer, open git bash and type the following, to clone the repository:

2.  *git clone “URL”*

 

Clone via SSH:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone git@github.com:I4-Projektseminar-HHU-2016/seminar-project-HaydarAk.git
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone via HTTPS:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/I4-Projektseminar-HHU-2016/seminar-project-HaydarAk.git
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 

More information on how to clone a repository can be found here:

https://git-scm.com/book/it/v2/Git-Basics-Getting-a-Git-Repository

 

-   **Download Repository ZIP:**

1.  In the web-view of the repository, click on the button\* Clone or Download *in the upper right corner*.\*

2.  Click on *Download as ZIP*

3.  Choose a download location

4.  Start Download & wait until it has finished.

5.  Extract ZIP File

 

 

### 2.2 Download full Wikipedia dump (Optional)

Database-Snippet is included in Repository, for testing purposes.

Full DB-Dump can be downloaded here:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
https://dumps.wikimedia.org/enwiki/20160720/
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Following files have to be downloaded:
enwiki-20160720-page.sql.gz  ~1.3 GB
enwiki-20160720-pagelinks.sql.gz 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 

Additionally \~30GB disk space is needed, if running with full Wikipedia DB-dump.

 


 

### 3. Installing / Running the Code

-   (Optional) *If DB-dump has been downloaded:* Replace the .gz files, with the files, you downloaded before.

 

-   **A: Running Script in Terminal:**

    Open a terminal window in the folder, where the files are saved in.

    Run *main\_file.py*

    Based on, how your OS & Python-Installation is configured, you should be able to run the Script with one of the following commands:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    python main_file.py

    py -3 main_file.py

    python3 main_file.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 

-   **B: Running Script in Editor’s / IDE’s**

    There is no *universal* way of how to run the script in different editors / IDEs.

    This fully depends on the Editor/IDE you use. Check the Editor/ IDE Documentation for further information.

 
 Note:
*More ​CPU Cores, RAM & fast Disk will result in an considerable improve of program execution time*

Authors
-------

-   **Haydar Akyürek** - [HaAky](https://github.com/HaAky)

 

License
-------

This project is licensed under the MIT License - see the [LICENSE.md] for more details(../master/LICENSE.md) .

 

Acknowledgments
---------------

-   \-

 

*template inspired by* <https://gist.github.com/PurpleBooth/109311bb0361f32d87a2>
