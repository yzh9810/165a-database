from template.table import Table, Record
from template.index import Index
from template.page import *
from template.config import *

import time

def current_milli_time():
    return round(time.time() * 1000)

# def set_offset_write(offset : list, values : list, page : Page):
#     if len(offset) != len(values):
#         print("Error: offset and list not match")
#         return False
#     for i in range(len(offset)):
#         print("write offset = ", offset[i])
#         print("value = ", values[i])
#         page.set_offset(offset[i])
#         page.write(values[i])
#     page.num_records = page.num_records + 1
#     return True
#
def bytearray_to_record(pagerange : PageRange, index : int, key_column: int, table : Table):
    offset = index
    Rid = int.from_bytes(pagerange.base_pages.physical_pages[RIDCol].data[offset : offset + ColSize], "little")
    key = int.from_bytes(pagerange.base_pages.physical_pages[key_column + 4].data[offset : offset + ColSize], "little")
    columns = []
    for i in range(table.num_columns):
        element = int.from_bytes(pagerange.base_pages.physical_pages[i + 4].data[offset: offset + ColSize], "little")
        columns.append(element)
    record = Record(Rid, key, columns)
    record.Indirection = int.from_bytes(pagerange.base_pages.physical_pages[IndirectionCol].data[offset : offset + ColSize], "little")
    record.Schema_Encoding = int.from_bytes(pagerange.base_pages.physical_pages[SchemaCol].data[offset : offset + ColSize], "little")
    record.time = int.from_bytes(pagerange.base_pages.physical_pages[TimestampCol].data[offset : offset + ColSize], "little")
    return record

def bytearray_to_record_tail(pageblocks : PageBlock, index : int, key_column: int, table : Table):
    offset = index
    Rid = int.from_bytes(pageblocks.physical_pages[RIDCol].data[offset : offset + ColSize], "little")
    key = int.from_bytes(pageblocks.physical_pages[key_column + 4].data[offset : offset + ColSize], "little")
    columns = []

    # print("offset = ", offset)
    for i in range(table.num_columns):
        # print("bytearray to record = ", pageblocks.physical_pages[i + 4].data[offset: offset + ColSize])
        element = int.from_bytes(pageblocks.physical_pages[i + 4].data[offset: offset + ColSize], "little")
        columns.append(element)
    record = Record(Rid, key, columns)
    record.Indirection = int.from_bytes(pageblocks.physical_pages[IndirectionCol].data[offset : offset + ColSize], "little")
    record.Schema_Encoding = int.from_bytes(pageblocks.physical_pages[SchemaCol].data[offset : offset + ColSize], "little")
    record.time = int.from_bytes(pageblocks.physical_pages[TimestampCol].data[offset : offset + ColSize], "little")
    return record



def RID_in_Tail_page(RID : int, pagerange : PageRange, table : Table):       ##find the Record based on RID in tail page
    # print("RID_in_Tail_page = ", RID)
    Index_Tail_page = RID // 4096 + 1
    Entry_Tail_page = RID % 4096
    record = bytearray_to_record_tail(pagerange.tail_pages[Index_Tail_page], Entry_Tail_page, table.key, table)
    return record


def Updated_Record(record : Record, pagerange : PageRange, table: Table):
    if record.Schema_Encoding == 0:
        return record
    else:
        while record.Schema_Encoding != 0:
            tail_record = RID_in_Tail_page(record.Indirection, pagerange, table)

            if tail_record == None:
                print("Error: didn't find anything")

            # print("tail page RID = ", tail_record.rid)

            record.Schema_Encoding = tail_record.Schema_Encoding
            record.Indirection = tail_record.Indirection
            # print("tail page indirection = ", tail_record.Indirection)
            for i in range(table.num_columns):
                if tail_record.columns[i] != none:
                    # print("tail_record.columns[i] != none:", tail_record.columns[i])
                    record.columns[i] = tail_record.columns[i]

    # print("record.columns = ", record.columns)
    return record



class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table : Table):
        self.table = table

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, key):
        Pagerange, offset = self.table.index.locate_by_PID(key)

        record = bytearray_to_record(Pagerange, offset, self.table.key, self.table)

        while record.Indirection != none:

            Index_Tail_page = record.Indirection // 4096 + 1
            Offset_Tail_page = record.Indirection % 4096
            #setting all delete tail page record RID as none
            Pagerange.tail_pages[Index_Tail_page].physical_pages[RIDCol].data[Offset_Tail_page: Offset_Tail_page + ColSize] = none.to_bytes(8, "little")

            tail_record = RID_in_Tail_page(record.Indirection, Pagerange, self.table)

            if tail_record == None:
                print("Error: didn't find anything")
                break

            # print("tail page RID = ", tail_record.rid)


        Pagerange.base_pages.physical_pages[IndirectionCol].offset_write(offset, none)
        Pagerange.base_pages.physical_pages[RIDCol].offset_write(offset, none)

        # print("record.columns = ", record.columns)



    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        # if len(self.table.page_directory) == 0:
        #         #     print("Need create new range page")
        #         #     self.table.insert_range_page()
        #         # if self.table.current_range_page != None:
        #         #     if self.table.current_range_page.has_capacity() == False:
        #         #         print("Need create new range page")
        #         #         self.table.insert_range_page()
        #         #
        #         # self.table.current_range_page.find_space()
        #         # RID = self.table.current_range_page.current_base_page.get_RID()
        #         # Indirection = 0
        #         # Schema = 0
        #         # Start_time = current_milli_time()
        #         # value = [RID, Indirection, Schema, Start_time]
        #         # for i in columns:
        #         #     value.append(i)
        #         #
        #         # offset = self.table.current_range_page.current_base_page.current_offset()
        #         # page = self.table.current_range_page.current_base_page
        #         #
        #         # set_offset_write(offset, value, page)
        #         # print(page.data)

        if len(self.table.page_directory) == 0:             ## if need need page range
            print("Need create new range page")
            self.table.insert_PageRange()
        if self.table.current_PageRange != None:
            if self.table.current_PageRange.has_capacity() == False:
                print("Need create new range page")
                self.table.insert_PageRange()

        #values to insert

        RID = self.table.current_PageRange.current_hold * ColSize + (self.table.num_range_page - 1) * PageSize
        Indirection = none
        Schema = 0
        Start_time = current_milli_time()
        records = columns
        num_cols = self.table.num_columns

        #start to insert
        if self.table.current_PageRange.insert_record_to_base(RID, Indirection, Schema, Start_time, records, num_cols):
            return True
        else:
            return False

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, key, column, query_columns):
        Records_loc, Records_PageRange = self.table.index.locate(column + 4, key)
        if len(Records_loc) == 0:
            print("Error: Fail to find the key")
            return False
        Records = []
        for i in range(len(Records_loc)):
            # print("page is", key)
            record = bytearray_to_record(Records_PageRange[i], Records_loc[i], column, self.table)
            # print("RID = ", record.rid," record indirection = ", record.Indirection)
            if record.rid == record.Indirection and record.rid == none:
                continue
            record = Updated_Record(record, Records_PageRange[i], self.table)

            Records.append(record)
        return Records


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, key, *columns):
        if len(self.table.page_directory) == 0:
            print("Error: Don't have page range at all!!!!!")
            return False

        Records_loc, Record_PageRange = self.table.index.locate(self.table.key + 4, key)

        if len(Records_loc) > 1:
            print("Error: find multiple keys!!!!")
            return False
        if len(Records_loc) == 0:
            print("Error: don't find the key in the Table")
            return False
        #
        page_range = list(Record_PageRange)[0]              ## find the Page Range have the key
        record = bytearray_to_record(page_range, Records_loc[0], self.table.key, self.table)
        # print("page_range.tail_pages[page_range.current_tail_pages].physical_pages[RIDCol].num_entries", page_range.tail_pages[page_range.current_tail_pages].physical_pages[RIDCol].num_entries)
        RID = page_range.tail_pages[page_range.current_tail_pages].physical_pages[RIDCol].num_entries * ColSize + PageSize*(page_range.current_tail_pages-1)   ##maybe need revise here
        # print("Rid in tail page is ", RID)
        Indirection = record.Indirection
        # print("record.Indirection is ", Indirection)
        Schema = record.Schema_Encoding
        temp = 0
        for i in columns:
            if i == None:
                temp = temp << 1
            else:
                temp = temp << 1 | 0b1

        # print("temp = ", bin(temp))
        Start_time = current_milli_time()
        # print("insert_record_to_tail RID = ", RID, "Indirection= ", Indirection, "Schema = ", bin(Schema), "columns = ", columns)
        page_range.insert_record_to_tail(RID, Indirection, Schema, Start_time, columns)

        ## important !! change indirection in BasePage here
        # print("base range Idirention ",Records_loc[0] * ColSize, "  Schema = ", Schema | temp)

        page_range.base_pages.physical_pages[IndirectionCol].offset_write(Records_loc[0], RID)
        page_range.base_pages.physical_pages[SchemaCol].offset_write(Records_loc[0], Schema | temp)

        return True



    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        self.table.index.create_index(self.table.key + 4)
        sum_value = 0
        for key in range(start_range, end_range + 1):
            for page_range in self.table.page_directory.values():
                try:
                    offset = self.table.index.indices.DataFrame[page_range][key]

                    record = bytearray_to_record(page_range, offset, self.table.key, self.table)
                    if record.rid == record.Indirection and record.rid == none:
                        continue
                    record = Updated_Record(record, page_range, self.table)
                    # print("find value with key =  ", key)

                    value = record.columns[aggregate_column_index]
                    # print("value =", value)

                except:
                    value = 0

                sum_value = sum_value + value
        return sum_value

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
