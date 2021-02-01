from template.page import *
from template.index import Index
from time import time



class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.Indirection = 0
        self.time = 0
        self.Schema_Encoding = 0
        self.key = key
        self.columns = columns

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.num_range_page = 0
        self.current_PageRange = None
        pass

    def insert_PageRange(self):
        self.num_range_page += 1
        self.page_directory[self.num_range_page] = PageRange(self.num_columns)
        self.current_PageRange = self.page_directory[self.num_range_page]
        if self.current_PageRange != None:
            print("Success create the range page")
        else:
            print("Fail to create the range page")

    def __merge(self):
        pass

