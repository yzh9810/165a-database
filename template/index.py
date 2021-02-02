from itertools import chain
from template.page import *
from template.config import *

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
class Index_struct:
    def __init__(self, ):
        self.PageRange_index = []
        self.DataFrame = {}
        self.key_column = None

    def DataFrame_dic_Make(self, column, table):
        self.key_column = column
        for Pagerange in table.PageRange_list:
            self.DataFrame[Pagerange] = {}
            self.PageRange_index.append(PageRange)

            pages = Pagerange.base_pages.physical_pages             #the columns in a pageblock
            for i in range(pages[0].num_entries):
                offset = ColSize * i
                key = int.from_bytes(pages[self.key_column].data[offset : offset + ColSize], "little")
                # print(page_value)
                self.DataFrame[Pagerange][key] = offset

    def DataFrame_dic_Insert(self, key, Pagerange: PageRange, offset : int):
        # print("self.PageRange_index" ,self.PageRange_index )
        if Pagerange not in self.PageRange_index:
            self.PageRange_index.append(Pagerange)
            self.DataFrame[Pagerange] = {}
        self.DataFrame[Pagerange][key] = offset


class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = None
        self.key_column = -1
    """
    # returns the location of all records with the given value on column "column"
    """
    def has_indices(self):
        if self.indices == None:
            return False
        else:
            return True


    def locate(self,  value : int):
        Record_Location = []
        Record_PageRange = []
        for Page_range in self.table.PageRange_list:
            try:
                offset = self.indices.DataFrame[Page_range][value]
                Record_Location.append(offset)
                Record_PageRange.append(Page_range)
            except:
                pass

        return Record_Location, Record_PageRange



    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_PID(self, PID: int):
        Pagerange = self.table.page_directory[PID // PageSize + 1]
        offset = PID % PageSize
        return Pagerange, offset


    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """
    def insert_index(self, keys : list, Pagerange : PageRange, offset: int):
        key = keys[self.key_column]
        self.indices.DataFrame_dic_Insert(key, Pagerange, offset)

    def create_index(self, key_column : int):
        self.indices = Index_struct()
        self.key_column = key_column
        self.indices.DataFrame_dic_Make(key_column, self.table)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self):
        self.indices = None
        self.key_column = -1