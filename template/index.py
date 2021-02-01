from BTrees.IOBTree import IOBTree
from itertools import chain
from template.page import *
from template.config import *
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
class Index_struct:
    def __init__(self):
        self.PageRange_index = []
        self.DataFrame = {}

    def Make_DataFrame_dic(self, column, Pagerange: PageRange):
        self.DataFrame[Pagerange] = {}
        pages = Pagerange.base_pages.physical_pages             #the columns in a pageblock
        for i in range(pages[0].num_entries):
            offset = ColSize * i
            key = int.from_bytes(pages[column].data[offset : offset + ColSize], "little")
            # print(page_value)
            self.DataFrame[Pagerange][key] = i


class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = Index_struct()

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column : int, value : int):
        # return_value = {}
        # return_page_range = []
        # for range_page in self.table.page_directory.values():
        #     for page in range_page.base_page:
        #         if page.empty == False:
        #             # print("numRecord = ", page.num_records)
        #             for record_index in range(page.num_records):
        #                 offset = column*col_size + col_size*record_index*(self.table.num_columns+4)
        #                 page_value = int.from_bytes(page.data[offset : offset + col_size], "little")
        #                 # print(page_value)
        #                 if page_value == value:
        #                     # print("i get the value", page_value)
        #                     return_value[page] = col_size*record_index*(self.table.num_columns+4)
        #                     return_page_range.append(range_page)
        #         # else:
        #         #     print("page is empty ", page)

        # print("key in locate = ", value)
        Record_Location = {}
        for pagerange in self.table.page_directory.values():
            pages = pagerange.base_pages.physical_pages             #the columns in a pageblock
            for i in range(pages[0].num_entries):
                offset = ColSize * i
                page_value = int.from_bytes(pages[column].data[offset : offset + ColSize], "little")
                # print("page_value = ", page_value)
                if page_value == value:
                    # print("i get the value", page_value)
                    Record_Location[pagerange] = i*ColSize
                # else:
                #     print("page is empty ", page)

        return Record_Location

    # def locate_tail_page(self, column, value : int):
    #     return_value = {}
    #     for range_page in self.table.page_directory.values():
    #         for page in range_page.base_page:
    #             if page.empty == False:
    #                 for record_index in range(page.num_records):
    #                     offset = column*col_size + col_size*record_index*(self.table.num_columns+4)
    #                     # print("offset = ", offset)
    #                     page_value = int.from_bytes(page.data[offset : offset + col_size], "little")
    #                     # print(page_value)
    #                     if page_value == value:
    #                         # print("i get the value", page_value)
    #                         return_value[page] = col_size*record_index*(self.table.num_columns+4)
    #             # else:
    #             #     print("page is empty ", page)
    #     return return_value

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_by_PID(self, PID: int):
        Pagerange = self.table.page_directory[PID // PageSize + 1]
        offset = PID % PageSize
        return Pagerange, offset


    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number, Pagerange : PageRange):
        self.indices.Make_DataFrame_dic(column_number, Pagerange)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass