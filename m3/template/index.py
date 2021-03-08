from itertools import chain
from template.page import *
from template.config import *

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
class Index_struct:
    def __init__(self, key_column):
        self.DataFrame = {}
        self.key_column = key_column


    def delete_key(self, key, PRname):
        del self.DataFrame[PRname][key]
        if len(self.DataFrame[PRname]) == 0:
            del self.DataFrame[PRname]


    def delete_key_offset(self, key, PRname, offset :int):
        try:
            if offset in self.DataFrame[PRname][key]:
                self.DataFrame[PRname][key].remove(offset)
                if len(self.DataFrame[PRname][key]) == 0:
                    self.delete_key(key, PRname)
            else:
                print("Error: don't find offset in index!!")
        except:
            print()



    def DataFrame_dic_MakePR(self, Pagerange):
        PRname = Pagerange.name
        if PRname not in self.DataFrame.keys():
            self.DataFrame[PRname] = {}
            pages = Pagerange.base_pages.physical_pages  # the columns in a pageblock
            for i in range(pages[0].num_entries):
                offset = ColSize * i
                key = int.from_bytes(pages[self.key_column].data[offset: offset + ColSize], "little")
                # print(page_value)
                if key not in self.DataFrame[PRname].keys():
                    self.DataFrame[PRname][key] = []
                self.DataFrame[PRname][key].append(offset)
        else:
            print("Error: the Page Range already in the index!!")




    def DataFrame_dic_Make(self, table):
        for PRname in table.PageRange_list.keys():
            Pagerange = table.PageRange_list[PRname]
            self.DataFrame[PRname] = {}

            pages = Pagerange.base_pages.physical_pages             #the columns in a pageblock
            for i in range(pages[0].num_entries):
                offset = ColSize * i
                key = int.from_bytes(pages[self.key_column].data[offset : offset + ColSize], "little")
                # print(page_value)
                if key not in self.DataFrame[PRname].keys():
                    self.DataFrame[PRname][key] = []
                self.DataFrame[PRname][key].append(offset)










    def DataFrame_dic_Insert(self, key, PRname, offset : int):
        # print("self.PageRange_index" ,self.PageRange_index )
        if PRname not in self.DataFrame.keys():
            self.DataFrame[PRname] = {}
        if key not in self.DataFrame[PRname].keys():
            self.DataFrame[PRname][key] = []
        self.DataFrame[PRname][key].append(offset)


class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = {}
    """
    # returns the location of all records with the given value on column "column"
    """
    def has_indices(self):
        if self.indices == {}:
            return False
        else:
            return True


    def locate(self, value : int, col : int):
        Record_Location = []
        Record_PageRange = []

        for PRname in self.indices[col].DataFrame.keys():
            try:
                offsets = self.indices[col].DataFrame[PRname][value]
                Record_Location.append(offsets)
                Record_PageRange.append(PRname)
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
    def insert_index(self,  keys : list, PRname, offset: int):
        for key_column in self.indices.keys():
            key_element = keys[key_column]
            self.indices[key_column].DataFrame_dic_Insert(key_element, PRname, offset)

    def create_index(self, key_column : int):
        self.indices[key_column] = Index_struct(key_column)
        self.indices[key_column].DataFrame_dic_Make(self.table)

    def update_index(self, prev_keys: list, PRname, offset : int, *new_keys):
        for i in range(len(new_keys)):
            if new_keys[i] != None and ((i + meta_data) in self.indices.keys()):
                self.indices[i + meta_data].delete_key_offset(prev_keys[i], PRname, offset)
                self.indices[i + meta_data].DataFrame_dic_Insert(new_keys[i], PRname, offset)

    def create_empty_index(self, key_column: int):
        self.indices[key_column] = Index_struct(key_column)

    def insert_PR_index(self, Pagerange, col_num : int):
        self.indices[col_num].DataFrame_dic_MakePR(Pagerange)
    """
    # optional: Drop index of specific column
    """

    def drop_index(self, col):
        if col in self.indices.keys:
            del self.indices[col]
        else:
            print("don't have the column in index")

    def delete_key(self, key_column: int, key, PRname):
        self.indices[key_column].delete_key(key, PRname)
