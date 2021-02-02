from template.table import Table, Record
from template.index import Index
from template.page import *
from template.config import *

import time

def current_milli_time():
    return round(time.time() * 1000)


def bytearray_to_record(pageblock : PageBlock, index : int, key_column: int):
    offset = index
    Rid = int.from_bytes(pageblock.physical_pages[RIDCol].data[offset: offset + ColSize], "little")
    key = int.from_bytes(pageblock.physical_pages[key_column + 4].data[offset: offset + ColSize], "little")

    columns = []
    for i in range(4, len(pageblock.physical_pages)):
        element = int.from_bytes(pageblock.physical_pages[i].data[offset: offset + ColSize], "little")
        columns.append(element)

    record = Record(Rid, key, columns)
    record.Indirection = int.from_bytes(pageblock.physical_pages[IndirectionCol].data[offset : offset + ColSize], "little")
    record.Schema_Encoding = int.from_bytes(pageblock.physical_pages[SchemaCol].data[offset : offset + ColSize], "little")
    return record




def TailPage_Record_From_RID(RID : int, pagerange : PageRange, table : Table):       ##find the Record based on RID in tail page
    # print("RID_in_Tail_page = ", RID)
    Index_Tail_page = RID // 4096 + 1
    Entry_Tail_page = RID % 4096
    record = bytearray_to_record(pagerange.tail_pages[Index_Tail_page], Entry_Tail_page, table.key_column)
    return record


def Find_Newest_Record(record : Record, pagerange : PageRange, table: Table):
    if record.Schema_Encoding == 0:
        return record
    else:
        while record.Schema_Encoding != 0:
            tail_record = TailPage_Record_From_RID(record.Indirection, pagerange, table)

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
        Record_Location, Record_PageRange = self.table.index.locate(key)


        for i in range(len(Record_PageRange)):
            record = bytearray_to_record(Record_PageRange[i].base_pages, Record_Location[i], self.table.key_column)
            while record.Indirection != none:

                Index_Tail_page = record.Indirection // 4096 + 1
                Offset_Tail_page = record.Indirection % 4096
                #setting all delete tail page record RID as none
                Record_PageRange[i].tail_pages[Index_Tail_page].physical_pages[RIDCol].data[Offset_Tail_page: Offset_Tail_page + ColSize] = none.to_bytes(8, "little")

                record = TailPage_Record_From_RID(record.Indirection, Record_PageRange[i], self.table)

                if record == None:
                    print("Error: didn't find anything")
                    break

        for i in range(len(Record_PageRange)):
            Record_PageRange[i].base_pages.physical_pages[IndirectionCol].offset_write(Record_Location[i], none)
            Record_PageRange[i].base_pages.physical_pages[RIDCol].offset_write(Record_Location[i], none)




    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        if len(self.table.PageRange_list) == 0:             ## if need need page range
            self.table.insert_PageRange()
        if self.table.current_PageRange != None:
            if self.table.current_PageRange.has_capacity() == False:
                self.table.insert_PageRange()
        # create index of column key
        if self.table.index.has_indices() == False:
            # print("create index!!")
            self.table.index.create_index(self.table.key_column + 4)

        #values to insert
        RID = self.table.current_PageRange.current_hold * ColSize + (self.table.num_range_page - 1) * PageSize
        Indirection = none
        Schema = 0
        Start_time = current_milli_time()
        records = columns
        num_cols = self.table.num_columns
        Record_Array = [RID, Indirection, Schema, Start_time]
        for i in records:
            Record_Array.append(i)

        #start to insert
        if self.table.current_PageRange.insert_record_to_base(RID, Indirection, Schema, Start_time, records, num_cols):
            # print("insert index",Record_Array, " into index" )
            self.table.index.insert_index(Record_Array, self.table.current_PageRange, (self.table.current_PageRange.current_hold - 1)*ColSize)

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
        if self.table.index.key_column != column + 4:
            print("select delete index")
            self.table.index.drop_index()
            self.table.index.create_index(column + 4)

        Record_Location, Record_PageRange = self.table.index.locate(key)
        Records = []
        for i in range(len(Record_PageRange)):
            record = bytearray_to_record(Record_PageRange[i].base_pages, Record_Location[i], self.table.key_column)
            if record.rid == record.Indirection and record.rid == none:
                continue
            record = Find_Newest_Record(record, Record_PageRange[i], self.table)
            Records.append(record)
        return Records


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, key, *columns):
        if len(self.table.PageRange_list) == 0:
            print("Error: Don't have page range at all!!!!!")
            return False

        Record_Location, Record_PageRange = self.table.index.locate(key)

        if len(Record_Location) > 1:
            print("Error: find multiple keys!!!!")
            return False
        if len(Record_Location) == 0:
            print("Error: don't find the key in the Table")
            return False
            #
        Page_range = list(Record_PageRange)[0]  ## find the Page Range have the key
        record = bytearray_to_record(Page_range.base_pages, Record_Location[0], self.table.key_column)
        # print("page_range.tail_pages[page_range.current_tail_pages].physical_pages[RIDCol].num_entries", page_range.tail_pages[page_range.current_tail_pages].physical_pages[RIDCol].num_entries)
        RID = Page_range.tail_pages[Page_range.current_tail_pages].physical_pages[
                  RIDCol].num_entries * ColSize + PageSize * (
                          Page_range.current_tail_pages - 1)  ##maybe need revise here
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
        Page_range.insert_record_to_tail(RID, Indirection, Schema, Start_time, columns)

        Page_range.base_pages.physical_pages[IndirectionCol].offset_write(Record_Location[0], RID)
        Page_range.base_pages.physical_pages[SchemaCol].offset_write(Record_Location[0], Schema | temp)

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
        sum_value = 0
        for key in range(start_range, end_range + 1):
            Record_Location, Record_PageRange = self.table.index.locate(key)
            if len(Record_Location) > 0:
                record = bytearray_to_record(Record_PageRange[0].base_pages, Record_Location[0], self.table.key_column)
                if record.rid == record.Indirection and record.rid == none:
                    continue
                record = Find_Newest_Record(record, Record_PageRange[0], self.table)
                # print("find value with key =  ", key)

                value = record.columns[aggregate_column_index]

                # print("value =", value)

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
