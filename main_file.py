import os.path
from SQLReader import work_on_file
from DB_handler import build_db

if __name__ == '__main__':
    pagelinks = 'enwiki-20160720-pagelinks.sql.gz'
    pages = 'enwiki-20160720-page.sql.gz'
    if os.path.isfile(pages) and os.path.isfile(pagelinks):
        work_on_file(pages, 'page')
        work_on_file(pagelinks, 'pagelinks')
        build_db('test_db')
    else:
        raise IOError("file not found")


# method for for generating snippets of sql.gz files#
def generate_file_snippets (in_file, out_file, len_num):
    import gzip
    try:
        sql_file = gzip.open(in_file, 'rb')
        save_file = gzip.open(out_file, 'wb')
    except IOError:
        print("file not found")
        raise

    end_line = '-- Dump completed on'
    line_number = 0
    while True:
        # tries reading line
        # if UnicodeDecodeError is raised, line is not encoded in utf-8
        #   line_number is added to list and function continues with next iteration
        try:
            line_number += 1
            line = sql_file.readline()
            print(line_number)
        except UnicodeDecodeError:
            continue

        if line_number <= len_num:
            save_file.write(line)
        try:
            if line.decode('utf-8').startswith(end_line):
                sql_file.close()
                save_file.write(line)
                save_file.close()
                break
        except UnicodeDecodeError:
            continue
