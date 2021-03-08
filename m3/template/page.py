from template.config import *


class Page:

    def __init__(self):
        self.num_entries = 0
        self.data = bytearray(PageSize)

    def has_capacity(self):
        if self.num_entries * ColSize < PageSize:
            return True
        else:
            return False

    def write(self, value):
        offset = self.num_entries * ColSize
        # print("value = ", value )
        new_entry = value.to_bytes(ColSize, byteorder='little')
        self.data[offset:offset + ColSize] = new_entry
        self.num_entries += 1

    def offset_read(self, offset):
        value = int.from_bytes(self.data[offset:offset + ColSize], "little")
        return value

    def offset_write(self, offset, value):
        # print("value = ", value)
        new_entry = value.to_bytes(ColSize, byteorder='little')
        self.data[offset:offset + ColSize] = new_entry

class PageBlock:
    """
    :param physical_pages: Page		#a set of physical pages , one for each column.
    """

    def __init__(self, num_cols):
        self.physical_pages = []
        for i in range((num_cols + 4)):
            self.physical_pages.append(Page())  # 4 for meta data columns

    def has_capacity(self):
        if self.physical_pages[0].has_capacity():  # all pages in one page block are consistant, no need to check other pages
            return True
        else:
            return False

class PageRange:
    """
    :param max_hold: int			#Maximum number of records held by one page range
    :param current_hold: int		#Number of records in the page range
    :param base_pages: PageBlock	#current base pages in the page range
    :param tail_pages: PageBlock	#current tail pages in the page range
    """

    def __init__(self, num_cols, name, Evict : int):
        self.current_hold = 0
        self.num_cols = num_cols
        self.base_pages = PageBlock(num_cols)
        self.current_tail_pages = 0
        self.tail_pages = {}
        self.add_new_tail()
        self.dirty = False
        self.name = name
        self.Evict = Evict
		self.pin = 0
		
	def pin_pagerange(self):
		self.pin += 1
		
	def unpin_pagerange(self):
		self.pin -= 1

    def has_capacity(self):
        if self.current_hold < PageSize / ColSize:
            return True
        else:
            return False

    def add_new_tail(self):
        self.current_tail_pages = self.current_tail_pages + 1
        self.tail_pages[self.current_tail_pages] = PageBlock(self.num_cols)

    def delete_tail_pages(self):
        self.current_tail_pages = 0
        self.tail_pages = {}


        self.add_new_tail()
    """
    Arguments:
        - num_cols: int
        number of columns EXCLUDING metadata columns
        - record:
            A list of values (e.g. [SID, grade1, grade2...])
        -
    """

    def insert_record_to_base(self, rid, Indirection : int, Schema : int, time_stamp : int, record : list, num_cols : int):

        if self.base_pages.has_capacity():
            self.base_pages.physical_pages[RIDCol].write(rid)
            self.base_pages.physical_pages[IndirectionCol].write(Indirection)
            self.base_pages.physical_pages[SchemaCol].write(Schema)
            self.base_pages.physical_pages[TimestampCol].write(time_stamp)

            for j in range(num_cols):
                # print("record[j] =", record[j])
                self.base_pages.physical_pages[4 + j].write(record[j])
            self.current_hold += 1
            return True
        else:
            print("Error: Current Page Range have no Space")
            return False


    """
    For updates only. there can be as many tail pages as needed for one pagerange. I am using the non-cumulative way
    Arguments:
        - value: int
        updated value
        - column: int
            column index to perform the update
        -
    """

    def insert_record_to_tail(self, rid, indirection, schema, time_stamp, column : list):

        # print("current tail page entry is = ", self.tail_pages[self.current_tail_pages].physical_pages[0].num_entries)
        if self.tail_pages[self.current_tail_pages].has_capacity() == False:
            # print("need to add new tail page")
            self.add_new_tail()

        self.tail_pages[self.current_tail_pages].physical_pages[RIDCol].write(rid)
        self.tail_pages[self.current_tail_pages].physical_pages[IndirectionCol].write(indirection)
        self.tail_pages[self.current_tail_pages].physical_pages[SchemaCol].write(schema)
        self.tail_pages[self.current_tail_pages].physical_pages[TimestampCol].write(time_stamp)
        for j in range(len(column)):
            if column[j] != None:
                # print("column[j] = ", column[j], " j  = ", j)
                self.tail_pages[self.current_tail_pages].physical_pages[4 + j].write(column[j])
            else:
                self.tail_pages[self.current_tail_pages].physical_pages[4 + j].write(none)

        return

    

    






