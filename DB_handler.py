import pickle
import glob
import sqlite3
import psutil
import os
import errno
import time

# reads & returns the amount of space of physical memory held by the process, this method is called in
def memory_usage():
    p = psutil.Process(os.getpid())                 # gets process_id from operating system
    used_mem = p.memory_info()[0] / float(2 ** 20)  # physical memory held by a process, value in bytes, for process p
    return round(used_mem, 2)                       # / float(2 ** 20) divides val by 2^20, converts bytes -> megabytes


# creates directory 'sub_dir'
# 'errno.EEXIST' (sub_dir already exists) exception will be ignored
def make_sure_path_exists(sub_dir):
    try:
        os.makedirs(sub_dir)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


# pagerank algorithm:
# doesnt calc correctly

def compute_ranks():
    d = 0.85
    # Test-dict equals Example 2 from:
    # http://www.cs.princeton.edu/~chazelle/courses/BIB/pagerank.htm
    test_dict = {'10': ['20', '30', '40'],
              '20': ['10'],
              '30': ['10'],
              '40': ['10','50', '60', '70', '80'],
              '50': [],
              '60': [],
              '70': [],
              '80': []
              }

    num_pages = len(test_dict)
    num_of_loops = 50
    n_ranks = {}
    ranks = {}
    for element in test_dict:
        ranks[element] = 1.0

    for _ in range(num_of_loops):
        for source in test_dict:
            newrank = (1-d)/num_pages
            for target in test_dict:
                if source in test_dict[target]:
                    newrank += d*ranks[target] / len(test_dict[target])
            n_ranks[source] = newrank
        ranks = n_ranks

    for e in ranks.items():
        print (e)


# inserts values from pagelinks dicts to db
def add_pagelinks(db_name):
    cnt = 0
    files = glob.glob("pagelinks_dict/*.pickle")
    conn = sqlite3.connect(db_name+'.db')
    c = conn.cursor()
    max_files = len(files)
    print (" --- inserting pagelinks dictionaries to db --- ")
    for file in files:
        cnt += 1

        print ("Inserting ", cnt, " of ", max_files, " dicts")
        t = pickle.load(open(file, 'rb'))
        for k, v in t.items():
            for value in v:
                c.execute('''INSERT INTO links VALUES(?,?)''', (k,value,))
    print("inserting done")
    print("creating index")
    c.execute("CREATE INDEX index_links ON links (source, target);")
    print("commiting")
    conn.commit()
    print ("done")
    conn.close()

# inserts values from pages dicts to db
def add_pages(db_name):
    cnt = 0
    files = glob.glob("page_dict/*.pickle")
    print (" --- inserting pages dictionaries to db --- ")
    max_files = len(files)
    conn = sqlite3.connect(db_name+'.db')
    c = conn.cursor()

    for file in files:
        cnt += 1
        print("Inserting ", cnt, " of ", max_files, " dicts")
        t = pickle.load(open(file, 'rb'))
        for k, v in t.items():
            c.execute('''INSERT INTO pages VALUES(?,?)''', (k, v,))
    print("inserting done")
    print("creating index")
    c.execute("CREATE INDEX index_pages ON pages (id, title);")
    print("commiting")
    conn.commit()
    print ("done")
    conn.close()


# creates db & table
def create_db(db_name):

    conn = sqlite3.connect(db_name+'.db')
    c = conn.cursor()
    try:
        c.execute('''CREATE TABLE links
                    (source text, target text)''')
        c.execute('''CREATE TABLE pages
                    (id text, title text)''')
        c.execute('''CREATE TABLE p_link_ids
                    (source_id text, target_id text)''')
        c.execute('''CREATE TABLE p_ranks
                    (p_id text, p_rank text, p_rank_new text)''')
        conn.commit()
    except sqlite3.OperationalError:
        pass

    conn.close()

# Joins values from pages & links  to p_link_ids, if links.target = pages.title
def join_tables(db_name):
    conn = sqlite3.connect(db_name+'.db')
    c = conn.cursor()
    print ("joining values from pages & links")
    c.execute('''INSERT into p_link_ids (source_id, target_id)
                 SELECT l.source, p.id FROM links l INNER JOIN pages p
                 ON l.target = p.title''')
    print("tables joined")
    print("creating index")
    c.execute("CREATE INDEX index_plinks ON p_link_ids (source_id, target_id);")
    c.execute("DROP TABLE links;")
    print("commiting")
    conn.commit()
    conn.close()

# fills pagerank table with IDs + start value for pagerank
def fill_p_ranks_table(db_name):
    conn = sqlite3.connect(db_name+'.db')
    c = conn.cursor()
    print("inserting p_ranks")
    c.execute('''INSERT into p_ranks (p_id, p_rank, p_rank_new)
                 SELECT DISTINCT (p_link_ids.source_id), '1.0', '1.0' FROM p_link_ids;''')
    print("inserting done")
    # c.execute("CREATE INDEX index_pranks ON p_ranks (p_id, p_rank);")
    print("commiting")
    conn.commit()
    print("done")
    print()
    conn.close()


# main func of file.
# coordinates method calls
def build_db(database_name):
    print("Building sqlite db. ")
    create_db(database_name)
    add_pages(database_name)
    add_pagelinks(database_name)
    join_tables(database_name)
    fill_p_ranks_table(database_name)
    print("Generating pagerank dicts for calc")
    generate_pagerank_dict(database_name)


    show_dict_snippet() # show snippet


# generates dictionaries for pagerank calc.
# dict structure:
# key: target_page
# values: list of page_ids, linking to target_page
def generate_pagerank_dict(db_name):
    make_sure_path_exists("p_rank_dict")
    conn = sqlite3.connect(db_name+'.db')
    cursor = conn.cursor()
    cnt = 0
    cursor.execute('''SELECT * FROM p_link_ids ORDER BY source_id ASC ''')
    prank_dict = {}
    tmp = 0
    while True:
        vals = cursor.fetchmany(5000)
        if len(prank_dict) > 50000 and vals[0][0] not in prank_dict:
            print (memory_usage())
            tmp += len(prank_dict)
            #print ("elements :", tmp)
            with open("p_rank_dict/dict"+str(cnt)+".pickle", 'wb') as pickle_file:
                pickle.dump(prank_dict, pickle_file, pickle.HIGHEST_PROTOCOL)
            prank_dict.clear()
            cnt += 1
        if len(vals) == 0:
            break

        for tuples in vals:
            if tuples[0] in prank_dict:
                # append the new value to existing list
                prank_dict[tuples[0]].append(tuples[1])
            else:
                # create a new value-list for key
                prank_dict[tuples[0]] = [tuples[1]]



def get_pageranks(db_name): # under construction
    t = pickle.load(open("p_rank_dict/dict1.pickle", "rb"))

    conn = sqlite3.connect(db_name+'.db')
    cursor = conn.cursor()
    query = 'select count(p_id) from p_ranks'
    cursor.execute(query)
    npages = cursor.fetchone()[0]
    tmp_dict = {}
    for element in t.items():
        tmp_dict = {}
        out_list = list(element[1])
        tmp_dict[element[0]]=out_list
        query = 'select p_id, p_rank from p_ranks where p_id in (' + ','.join(map(str, out_list)) + ')'
        cursor.execute(query)
        ranks = {}
        results = cursor.fetchall()
        for r in results:
            ranks[r[0]]=r[1]
        break


# shows 5 pagerank_dict snippet
def show_dict_snippet():
    time.sleep(2)
    print ("showing snippet of a pagerank dict")
    files = glob.glob("p_rank_dict/*.pickle")
    try:
        tmp = pickle.load(open(files[0], 'rb'))
        cnt = 0
        print (" \n \ntarget-page \t pages linking to target-page")
        for element in tmp.keys():
            print (element, '\t\t\t', tmp[element])
            cnt += 1
            if cnt > 5:
                break
    except IOError:
        print ("no file for snippet")