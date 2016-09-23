REQUIEREMENTS: 
  - ~30GB disk space, if running with full wikipedia dump.
  - python 3.5 with following libraries:
    pickle,glob, sqlite3, psutil, os, errno, time

HOW TO RUN?
  run main_file.py in terminal / editor
  
  

IMPORTANT NOTE 1:
- project is still incomplete. PageRank wont be calculated (yet). 
  more information in projekt_dokumentation.pdf, chapter 6. "Reflexion & Fazit"

- Delete existing test_db.db file from folder, before running script.
- script prints, all progress it does, in terminal
- at the end, the script prints a snippet of the dictionary, it'll use for PageRank.

Missing:
- PageRank Calculation
- Visualisation of results

IMPORTANT NOTE 2:
  the .gz files in project folder are snippts of the the full sql-dumps and can be used for testing.

  full dumps available at  :
  https://dumps.wikimedia.org/enwiki/20160720/ 
  
  Delete the .gz files, then download
    enwiki-20160720-page.sql.gz  (1.3 GB) 
    and
    enwiki-20160720-pagelinks.sql.gz (4.8 GB) 
    from the link above and put them into projects main folder
  
IMPORTANT NOTE 3: 
  program execution may take > 1h with full dumps.

