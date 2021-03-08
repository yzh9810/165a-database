import sys
# sys.path.append("..")
# sys.path.append(".")
from template.table import Table
from template.config import *
from template.read_write_file import write_file, write_tableinfo, write_PageRange_list, read_tableinfo, read_PageRange_list
from os import path
from os import chdir
from os import mkdir
import shutil
import os
import pickle

class Database():

    def __init__(self):
        self.tables = {}
        self.curr_table = None
        self.disk = None
        pass

    def open(self, my_path):
        self.disk = my_path
        if path.exists(my_path):
            chdir(my_path)
            # print("Current working directory", os.getcwd())
        else:
            print(my_path, " not found...")
            mkdir(my_path)
            chdir(my_path)
            # print("Current working directory", os.getcwd())

    def close(self):
        for tablename in self.tables.keys():
            write_tableinfo(self.tables[tablename])
            write_PageRange_list(self.tables[tablename])

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        if path.exists(name):
            print("Table with that name already exists! Choose another name or drop table: ", name)
            exit(1)

        if self.disk != None:
            mkdir(name)


        table = Table(name, num_columns, key)
        self.tables[name] = table
        self.curr_table = table
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables.keys():
            del self.tables[name]
        if path.exists(name):
            shutil.rmtree(name)

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if path.exists(name):
            #get table information
            info_Dic = read_tableinfo(name)
            # print("info_Dic = ", info_Dic)
            name, num_columns, key = info_Dic["basic_inf"]
            Full_Pnames = list(info_Dic.keys())
            Full_Pnames.remove("basic_inf")
            Pnames = []
            for i in Full_Pnames:
                Pnames.append(int(i.split('.')[0]))
            # print(Pnames)

            table = Table(name, num_columns, key)
            self.tables[name] = table
            self.curr_table = table
            read_PageRange_list(table, Pnames)
            return table
        else:
            print("Table with that name not exists! Using create_table instead: ", name)
            exit(1)




