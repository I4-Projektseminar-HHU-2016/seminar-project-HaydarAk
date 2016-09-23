# -*- coding: utf-8 -*-
import gzip
import time
import psutil
import glob
import multiprocessing
from multiprocessing import Process
import pickle
import os
import errno
from os import path


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


# opens file with utf-8 encoding, reads line by line and returns line number of unreadable_lines
# used for file_tests
def test_if_full_unicode(file_name, table_name):
    line_number = 0
    unreadablle_lines = []
    l = None
    err_file = open("testfile", 'wb')
    sql_file = gzip.open(file_name, 'rb')
    while True:
        try:
            line_number += 1

            l = sql_file.readline()
            line = l.decode('utf-8')
            if (line_number % 1000) == 0:
                print(line_number)

            if line.startswith('-- Dump completed'):
                print("Finished")
                sql_file.close()
                break

        except UnicodeDecodeError:
            unreadablle_lines.append(line_number)
            err_file.write(l)
            continue

        except Exception:
            raise

    sql_file.close()
    print("unreadable lines =", len(unreadablle_lines))
    print(unreadablle_lines)
    test_read(file_name, table_name, unreadablle_lines)


def test_read(file_name, table_name, list_unreadable_lines):
    sql_file = gzip.open(file_name, 'rb')
    sql_prefix = "INSERT INTO `" + table_name + "` VALUES "
    sql_suffix = ";"
    lines = 0
    r_dict = {}

    while True:
        try:
            line = sql_file.readline().decode('utf-8', 'ignore')
            lines += 1

            if lines in list_unreadable_lines:
                if line == "" or line.startswith("--"):
                    continue

                elif not (line.startswith(sql_prefix) or line.endswith(sql_suffix)):
                    continue

                else:
                    res = {}
                    tmp_list = []
                    values = line[len(sql_prefix)-1:len(line)-(len(sql_suffix))]

                    tmp_results = test_parse(values)
                    if len(tmp_results) >= 1:
                        for element in tmp_results:
                            if element[1] == '0':
                                tmp_list.append((element[0], element[2]))

                    res.update(dict(tmp_list))
                    for element in res.keys():
                        if element in r_dict:
                            print("ID: ", element, "page_title: ", r_dict[element], "   linking to: ", res[element])

            if lines > list_unreadable_lines[-1]:
                print("re-reading done")
                return

        except Exception:
            print("err in open_iso_files")
            raise


# reader function for MySQl file dumps
# reads file: every line is ignored, except for 2 cases:
#   --  lines containing values: puts the values into one of the 4 queues
#   --  The line that indicates end of file_dump: stops reading
# arguments:
# file_name = file name of sql-dump,
# table_name = table table of INSERT INTO lines from sql_dump
# l1_queue: queue, in which value_lines are put in, for further processing
def read_file(file_name, table_name, l1_queue, pros):

    sql_prefix = "INSERT INTO `" + table_name + "` VALUES "     # INSERT Lines of sql_dump beginn with
    sql_suffix = ";"                                            # INSERT Lines of sql_dump end with
    end_line = '-- Dump completed on'                           # pattern indicates end of dump

    line_number = 0
    elements = 0                                                # manager variable for equally filling queues
    unreadable_lines = []                                       # list of line numbers, which could not be read

    make_sure_path_exists(table_name)                           # create folder for file

    try:
        sql_file = gzip.open(file_name, 'rb')
    except IOError:
        print("file not found")
        raise

    # streams over zipped file, reads line by line and performs different actions
    # see comments below for details
    while True:
        # tries reading line
        # if UnicodeDecodeError is raised, line is not encoded in utf-8
        #   line_number is added to list and function continues with next iteration
        try:
            line_number += 1
            line = sql_file.readline().decode('utf-8', 'ignore')
        except UnicodeDecodeError:
            unreadable_lines.append(line_number)
            continue

        if line_number % 100 == 0 and line_number > 0:
            print("reached line:", line_number, "  Queue: ", l1_queue.qsize())

        # if true, rached end of dump
        # call open_iso_files, if unreadable lines are found.
        # put 'DONE' to end of each queue, which indicates, no more values are coming
        # exits function
        if line.startswith(end_line):
            sql_file.close()
            for _ in range(pros):
                l1_queue.put('DONE')
            return

        # lines starting with -- are comment lines.
        # blank lines and comment lines contain no values --> skip lines
        elif line == "" or line.startswith("--"):
            continue

        # beginning or end of line doesnt match prefix / sufix --> line is not part of an INSERT line --> skip line
        elif not (line.startswith(sql_prefix) or line.endswith(sql_suffix)):
            continue

        # if non of above: line is INSERT line
        # strip prefix & suffix from line:
        # add value to appropiate queue
        # variable tik_tok manages queues, so that every queue is evenly filled
        else:
            value = line[len(sql_prefix)-1:len(line)-(len(sql_suffix))]        #
            elements += 1
            l1_queue.put(value)


# sql parser function: gets line from queue, parses sql line and returns a list, containing all values as tuples.
# args:
#   l_queue: queue, from which function reads lines
#   val: for file naming and process identification useses
#   table_name: table name of previously read file. used for file naming
#   p_queue: function puts 'DONE' string, after finishing. Needed for process handling
#   mem_cap: max usable physical memory
def parse_input(l_queue, val, table_name, p_queue, mem_cap):
    parse_counter = 0
    file_num = 0
    results = []

    # if element in queue equals 'DONE' : pickles data to disc and exits.
    while True:
        values = l_queue.get()
        if values == 'DONE':
            file_name = table_name + "_" + str(val) + "_" + str(file_num) + '.pickle'
            full_path = path.relpath(table_name+"/"+file_name)
            try:
                with open(full_path, 'wb') as pickle_file:
                    pickle.dump(results, pickle_file, pickle.HIGHEST_PROTOCOL)
                    results.clear()
            except FileNotFoundError:
                print("cant save parsed pickle file")
                raise
            p_queue.put('DONE')

            print("process", val, ": parsing done")
            break

        # variable inits for counting and stuff
        parse_counter += 1
        values = values[1:-1]   # remove blanks
        tmp_results = []
        tuples = ()
        tokens = -1
        state = 0
        values.lstrip(" ")

        # file parsing starts here
        # loop works like a finite state mashine. Loops & parses symbol by symbol
        #
        for index, symbol in enumerate(values):
            if state == 0:
                if symbol == '(':
                    state = 1
                else:
                    raise ValueError("state: ", state, " character: ", symbol)
                continue

            elif state == 1:
                if '0' <= symbol <= '9' or symbol == '-' or symbol == '.':
                    state = 2
                elif symbol == '\'':
                    state = 3
                elif symbol == 'N':
                    state = 5
                elif symbol == ')':
                    tmp_results.append(tuples)
                    tuples = ()
                    state = 8
                else:
                    raise ValueError("state: ", state, " character: ", symbol)

                tokens = index
                if state == 3:
                    tokens += 1
                continue

            elif state == 2:
                if '0' <= symbol <= '9' or symbol == '-' or symbol == '.':
                    continue
                elif symbol == ',' or symbol == ')':
                    tmp_str = values[tokens: index]
                    tokens = -1
                    tuples += (tmp_str,)
                    if symbol == ',':
                        state = 7
                    elif symbol == ')':
                        tmp_results.append(tuples)
                        tuples = ()
                        state = 8
                else:
                    raise ValueError("state: ", state, " character: ", symbol)
                continue

            elif state == 3:
                if symbol == '\'':
                    tmp_str = values[tokens: index]
                    tokens = -1
                    if '\\' in tmp_str:
                        tmp_str = tmp_str.replace("\\", "")  # Unescape backslashed characters
                    tuples += (tmp_str,)
                    state = 6
                elif symbol == '\\':
                    state = 4
                continue

            elif state == 4:
                if symbol == '\'' or symbol == '"' or symbol == '\\':
                    state = 3
                else:
                    raise ValueError("state: ", state, " character: ", symbol)
                continue

            elif state == 5:
                if 'A' <= symbol <= 'Z':
                    continue
                elif symbol == ',' or symbol == ')':
                    if values[tokens:index] == "NULL":
                        tuples += (None,)
                    else:
                        raise ValueError("state: ", state, " character: ", symbol)
                    tokens = -1
                    if symbol == ',':
                        state = 7
                    elif symbol == ')':
                        tmp_results.append(tuples)
                        tuples = ()
                        state = 8
                else:
                    raise ValueError("state: ", state, " character: ", symbol)
                continue

            elif state == 6:
                if symbol == ',':
                    state = 7
                elif symbol == ')':
                    tmp_results.append(tuples)
                    tuples = ()
                    state = 8
                else:
                    raise ValueError("state: ", state, " character: ", symbol)
                continue

            elif state == 7:
                if '0' <= symbol <= '9' or symbol == '-' or symbol == '.':
                    state = 2
                elif symbol == '\'':
                    state = 3
                elif symbol == 'N':
                    state = 5
                else:
                    raise ValueError("state: ", state, " character: ", symbol)

                tokens = index
                if state == 3:
                    tokens += 1
                continue

            elif state == 8:
                if symbol is ',':
                    state = 9
                else:
                    raise ValueError("state: ", state, " character: ", symbol)
                continue
            elif state == 9:
                if symbol == '(':
                    state = 1
                else:
                    raise ValueError("state: ", state, " character: ", symbol)
                continue

        if table_name == 'page':
            for element in tmp_results:
                if element[1] == '0':
                    tmp_tuple = (element[0], element[2])
                    results.append(tmp_tuple)
        else:
            for element in tmp_results:
                if element[1] == '0' and element[3] == '0':
                    tmp_tuple = (element[0], element[2])
                    results.append(tmp_tuple)

        if memory_usage() >= mem_cap:
            file_name = table_name+"_"+str(val)+"_"+str(file_num)+'.pickle'
            full_path = path.relpath(table_name+"/"+file_name)
            try:
                with open(full_path, 'wb') as pickle_file:
                    pickle.dump(results, pickle_file, pickle.HIGHEST_PROTOCOL)
                    results.clear()
            except FileNotFoundError:
                print("can't save parsed pickle file")
                raise
            file_num += 1
        l_queue.task_done()


# converts list of tuples to dict, for list from pagelinks.sql.gz
# keys: ID of an article, who has outgoing links
# value: list of article titles, the article in key is linking to
# args:
#   d_queue: a queue with file names of pickled lists from parse_input()
#   mem_cap: max. ram, function is allowed to use
def links_list_to_dict(mem_cap, val, file_list):
    file_number = 0
    result_dict = {}
    cnt = 0
    ele = 0
    fil_max = len(file_list)
    # loop: opens every file in list and adds saved list of tuples to dict.
    for file in file_list:
        cnt += 1
        # open file and load pickled list
        # print("handling file ", file)
        tmp_list = (pickle.load(open(file, "rb")))
        ele += len(tmp_list)
        print("Process ", val, " || reading file: ", cnt, " of ", fil_max)
        # if dict gets too big: save & clear dict
        if memory_usage() >= mem_cap:
            ele += len(result_dict)
            full_path = path.relpath("pagelinks_dict/dict_"+str(val)+str(file_number)+'.pickle')
            with open(full_path, 'wb') as pickle_file:
                pickle.dump(result_dict, pickle_file, pickle.HIGHEST_PROTOCOL)
                result_dict.clear()
                file_number += 1

        # else add key, values to dict.
        else:
            for element in tmp_list:
                if element[0] in result_dict:
                    # append the new value to existing list
                    result_dict[element[0]].append(element[1])
                else:
                    # create a new value-list for key
                    result_dict[element[0]] = [element[1]]
    full_path = path.relpath("pagelinks_dict/dict_"+str(val)+str(file_number)+'.pickle')
    with open(full_path, 'wb') as pickle_file:
        print("Process ", val, " ||    finishing pagelinks dict...")
        pickle.dump(result_dict, pickle_file, pickle.HIGHEST_PROTOCOL)
        result_dict.clear()


# converts list of tuples to dict, for list from page.sql.gz
# keys: ID of an article
# value: title of the same article
# args:
#   d_queue: a queue with file names of pickled lists from parse_input()
#   mem_cap: max. ram, function is allowed to use
def page_list_to_dict(mem_cap, val, file_list):
    file_number = 0
    result_dict = {}
    cnt = 0
    ele = 0
    fil_max = len(file_list)
    # loop: if element in queue is 'DONE', no more files to read --> function saves current dict to file & terminates.
    # otherwise: converting list of tuples to dict.
    for file in file_list:
        # reading file & updating dict with tuples from file
        tmp_list = (pickle.load(open(file, "rb")))
        result_dict.update(dict(tmp_list))
        cnt += 1
        print("Process ", val, " || Processed files: ", cnt, " of ", fil_max )

        # check memory usage: if dict gets to big, save and clear
        if memory_usage() >= mem_cap:
            print("saving pages dict...")
            full_path = path.relpath("page_dict/dict_"+str(val)+str(file_number)+'.pickle')
            with open(full_path, 'wb') as pickle_file:
                pickle.dump(result_dict, pickle_file, pickle.HIGHEST_PROTOCOL)
                result_dict.clear()
                file_number += 1
    full_path = path.relpath("page_dict/dict_"+str(val)+str(file_number)+'.pickle')
    with open(full_path, 'wb') as pickle_file:
        print("Process ", val, " ||    finishing pages dict...")
        pickle.dump(result_dict, pickle_file, pickle.HIGHEST_PROTOCOL)
        result_dict.clear()
        file_number += 1


# makes list of tuples to dicts. starts 1-n processes for dicting pickle files.
# calls page_list_to_dict() or link_list to dict() based on, which file's lists have to be converted
# splits pickle-files between processes evenly
# uses 50% of free ram, dict size is based on this value
def generate_dicts(table_name):
    import numpy
    start = time.time()
    free_mem = round(((psutil.virtual_memory()[1])/1024**2), 2)
    free_mem = round((free_mem*0.5), 2)
    cpus = psutil.cpu_count()
    processes = []

    if table_name is 'page':
        make_sure_path_exists("page_dict/")
        if cpus >= 2:
            file_list = numpy.split(numpy.array(glob.glob('page/*.pickle')), cpus)
            for i in range(cpus):
                p = Process(target=page_list_to_dict, args=(free_mem/cpus, i, file_list[i]))
                processes.append(p)
        else:
            file_list = glob.glob('page/*.pickle')
            p = Process(target=page_list_to_dict, args=(free_mem, 0, file_list))
            processes.append(p)

    else:
        make_sure_path_exists("pagelinks_dict")
        if cpus >= 2:
            file_list = numpy.split(numpy.array(glob.glob('pagelinks/*.pickle')), cpus)
            for i in range(cpus):
                p = Process(target=links_list_to_dict, args=(free_mem/cpus, i, file_list[i]))
                processes.append(p)

        else:
            file_list = glob.glob('pagelinks/*.pickle')
            p = Process(target=links_list_to_dict, args=(free_mem, 0, file_list))
            processes.append(p)

    # starts processes
    for p in processes:
        p.start()
    # waits for processes to finish
    for p in processes:
        p.join()
    print("Generated dictionary for ", table_name, " in ", (time.time()-start)/60, " minutes")
    return True


# main function of file.
# handles part of processing & method calling
# returns True after reading, parsing and making dictionaries
def work_on_file(file_name, table_name):
    start = time.time()
    processes = []
    print ('\n\n')
    num_of_processes = psutil.cpu_count()
    if num_of_processes < 2:
        num_of_processes = 2

    free_mem = round(((psutil.virtual_memory()[1])/1024**2), 2)
    queue_length = int(free_mem/8)
    print("Total free memory: ", round(free_mem), "MB       queue length:", queue_length)

    line1_queue = multiprocessing.JoinableQueue(1200)
    p_queue = multiprocessing.JoinableQueue(4)

    process_mem = round(free_mem*0.25)
    print("25% of free memory is used for ", num_of_processes, " processes: ", process_mem, "MB")

    for i in range(num_of_processes):
        p = Process(target=parse_input, args=(line1_queue, i, table_name, p_queue, process_mem/num_of_processes))
        processes.append(p)

    for pro in processes:
        pro.start()
    print("Started", num_of_processes, " additional Processes \n")

    print("Reading file", file_name)
    read_file(file_name, table_name, line1_queue, num_of_processes)

    print("reading ", file_name, " done")
    print("waiting for processes to finish")

    while 1:
        time.sleep(2)
        print("elements left in queue: ", line1_queue.qsize())
        if line1_queue.qsize() == 0:
            if (any(p.is_alive() for p in processes)) and p_queue.qsize() < 4:
                continue
            else:
                break
    print("Parsing time for ", file_name, ": ", (time.time()-start)/60, " minutes")
    print("----------")
    return generate_dicts(table_name)


