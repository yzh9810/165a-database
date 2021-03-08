from template.page import *
from template.index import Index
from template.read_write_file import write_file, read_file
from os import chdir
from os import path
import time
import threading




class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.Indirection = 0
        self.Schema_Encoding = 0
        self.key = key
        self.columns = columns

class lock_manager:
   def __init__():
	   xLocks = []
	   sLocks = []
	   
   def add_xLock(self,new_xLock):
	   self.xLocks.append(new_xLock)
	   
   def add_sLock(self,new_sLock):
	   self.sLocks.append(new_sLock)
	   
   def release_xLock(self,locks):
	   for xlock in xLocks:
		   for lock in locks:
			   if xlock == locks:
				   self.xLocks.remove(xlock)
	 
	def release_sLock(self,locks):
	   for slock in sLocks:
		   for lock in locks:
			   if slock == lock:
				   self.sLocks.remove(slock)
	
   
	def acquire_xlock(self,lock):
	   for xlock in self.xLocks
		   if xlock == lock:
			   return False
			else:
				self.xLocks.append(lock)
	   return True
	
	def find_slock(self,lock):
		for slock in self.sLocks
			if slock == lock:
				return False
		return True

def bytearray_to_record(pageblock : PageBlock, index : int, key_column: int):
    offset = index
    Rid = int.from_bytes(pageblock.physical_pages[RIDCol].data[offset: offset + ColSize], "little")
    key = int.from_bytes(pageblock.physical_pages[key_column + meta_data].data[offset: offset + ColSize], "little")

    columns = []
    for i in range(4, len(pageblock.physical_pages)):
        element = int.from_bytes(pageblock.physical_pages[i].data[offset: offset + ColSize], "little")
        columns.append(element)

    record = Record(Rid, key, columns)
    record.Indirection = int.from_bytes(pageblock.physical_pages[IndirectionCol].data[offset : offset + ColSize], "little")
    record.Schema_Encoding = int.from_bytes(pageblock.physical_pages[SchemaCol].data[offset : offset + ColSize], "little")
    return record


def current_milli_time():
    return round(time.time() * 1000)





def TailPage_Record_From_RID(RID : int, pagerange : PageRange, table):       ##find the Record based on RID in tail page
    # print("RID_in_Tail_page = ", RID)
    Index_Tail_page = RID // 4096 + 1
    Entry_Tail_page = RID % 4096
    record = bytearray_to_record(pagerange.tail_pages[Index_Tail_page], Entry_Tail_page, table.key_column)
    return record


def Find_Newest_Record(record , pagerange : PageRange, table):
    if record.Schema_Encoding == 0:
        return record
    else:
        while record.Schema_Encoding != 0:
            tail_record = TailPage_Record_From_RID(record.Indirection, pagerange, table)

            if tail_record == None:
                print("Error: didn't find anything")

            # print("tail page RID = ", tail_record.rid)

            record.Schema_Encoding = record.Schema_Encoding-(record.Schema_Encoding & tail_record.Schema_Encoding)
            record.Indirection = tail_record.Indirection
            # print("tail page indirection = ", tail_record.Indirection)
            for i in range(table.num_columns):
                if tail_record.columns[i] != none:
                    # print("tail_record.columns[i] != none:", tail_record.columns[i])
                    record.columns[i] = tail_record.columns[i]

    # print("record.columns = ", record.columns)
    return record



class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.key_column = key
        self.num_columns = num_columns
        # self.page_directory = {}
        self.PageRange_list = {}
        self.index = Index(self)
        self.num_range_page = 0
        self.current_PageRange = None
        self.PageRangename_Set = set()
        self.GobalEvict = 0
		self.lock_manager=lock_manager()
		self.pinned_pageranges = [] #the pagerange is pinned when it is required by a query
		self.lock = threading.Lock()

    def Create_PageRange(self):
        name = None
        for i in range(Max_pagerange):
            if i not in self.PageRangename_Set:
                name = i
                break
        if name == None:
            print("Error: reach the maxium pagerange!!!")

        if self.num_range_page == BufferSize:
            Oldest_PageRange = list(self.PageRange_list.values())[0]
            for Pagerange in self.PageRange_list.values():
                if Oldest_PageRange.Evict > Pagerange.Evict and Pagerange.pin == 0:
                    Oldest_PageRange = Pagerange
            self.drop_PageRange(Oldest_PageRange)
            del self.PageRange_list[Oldest_PageRange.name]
            self.num_range_page -= 1

        self.GobalEvict = self.GobalEvict + 1
        self.PageRange_list[name] = PageRange(self.num_columns, name, self.GobalEvict)
        self.PageRangename_Set.add(name)
        self.current_PageRange = self.PageRange_list[name]
        self.num_range_page += 1

        if self.current_PageRange != None:
            # print("Success create the range page")
            pass
        else:
            print("Error: Fail to create the page range")


    def drop_PageRange(self, Pagerange : PageRange):
        if Pagerange.dirty == True:
            if path.exists(self.name) == False:
                print("Error: buffer full but don't have the disk to write!!!")
            chdir(self.name)
            self.merge(Pagerange)
            filename = "{}.prange".format(Pagerange.name)
            write_file(filename, Pagerange)
            chdir("..")




    def Load_PageRange(self, name : int):
        if self.num_range_page == BufferSize:
            Oldest_PageRange = list(self.PageRange_list.values())[0]
            for Pagerange in self.PageRange_list.values():
                if Oldest_PageRange.Evict > Pagerange.Evict and Pagerange.pin == 0:
                    Oldest_PageRange = Pagerange
            self.drop_PageRange(Oldest_PageRange)
            del self.PageRange_list[Oldest_PageRange.name]
            self.num_range_page -= 1
        self.GobalEvict = self.GobalEvict + 1
        self.PageRange_list[name] = PageRange(self.num_columns, name, self.GobalEvict)
        if name not in self.PageRangename_Set:
            self.PageRangename_Set.add(name)
        self.current_PageRange = self.PageRange_list[name]
        self.num_range_page += 1

        filename = str(name) + ".prange"
        chdir(self.name)
        read_file(filename, self.current_PageRange)
        chdir("..")




    def insert(self, *columns):
        if len(self.PageRange_list) == 0:             ## if need need page range
            self.Create_PageRange()
        if self.current_PageRange != None:
            if self.current_PageRange.has_capacity() == False:
                find = False
                for Pagerange in list(self.PageRange_list.values()):
                    if Pagerange.has_capacity() == True:
                        self.current_PageRange = Pagerange
                        find = True
                        #Pagerange.pin += 1 ###update pin
                        pinned_page = Pagerange
                        break
                if find == False:
                    self.Create_PageRange()
					## update on pin
#					for Pagerange in list(self.PageRange_list.values()):
#						if self.current_PageRange == Pagerange:
#							Pagerange.pin += 1
#							pinned_page = Pagerange
        # create index of column key
        if self.index.has_indices() == False:
            print("create index!!")
            self.index.create_index(self.key_column + meta_data)

        #values to insert
        RID = self.current_PageRange.current_hold * ColSize + (self.num_range_page - 1) * PageSize
        Indirection = none
        Schema = 0
        Start_time = current_milli_time()
        records = columns
        num_cols = self.num_columns
        Record_Array = [RID, Indirection, Schema, Start_time]
        for i in records:
            Record_Array.append(i)

        #start to insert
        if self.current_PageRange.insert_record_to_base(RID, Indirection, Schema, Start_time, records, num_cols):
            # print("insert index",Record_Array, " into index" )
            self.index.insert_index( Record_Array, self.current_PageRange.name, (self.current_PageRange.current_hold - 1)*ColSize)
            self.current_PageRange.dirty = True
            self.GobalEvict = self.GobalEvict + 1
            self.current_PageRange.Evict = self.GobalEvict
            return True
            #upin page
#            i = self.PageRange_list.index(pinned_page)
#            self.PageRange_list[i].pin -= 1
        else:
			#upin page
#            i = self.PageRange_list.index(pinned_page)
#            self.PageRange_list[i].pin -= 1
            return False


    def select(self, key, column, query_columns):
        if column + meta_data not in self.index.indices.keys():
            print("create new index")
            self.index.create_index(column + meta_data)

        Record_Location, Record_PageRangeName = self.index.locate(key, column + meta_data)
        if len(Record_Location) == 0:
            print("Error: don't find the key in select!!")
            print("key = ", key)
            exit(1)

        self.GobalEvict = self.GobalEvict + 1
        Records = []
        NeedLoadPageRangeName = set()
        for i in range(len(Record_PageRangeName)):
            if Record_PageRangeName[i] in self.PageRange_list.keys():
                for j in range(len(Record_Location[i])):
                    record = bytearray_to_record(self.PageRange_list[Record_PageRangeName[i]].base_pages, Record_Location[i][j], self.key_column)
                    # print("record rid = ", record.rid)
                    # print("record Indirection in base page = ", record.Indirection)
                    # print("record Schema_Encoding = ", bin(record.Schema_Encoding))
                    # print("record columns = ", record.columns)
                    if record.rid == record.Indirection and record.rid == none:
                    
						
						
                        print("find delete element!!")
                        continue
                    record = Find_Newest_Record(record, self.PageRange_list[Record_PageRangeName[i]], self)
                    Records.append(record)

                self.PageRange_list[Record_PageRangeName[i]].Evict = self.GobalEvict
            else:
                NeedLoadPageRangeName.add(i)

        for i in NeedLoadPageRangeName:
            self.Load_PageRange(Record_PageRangeName[i])
            ## after insert current_pagerange is the new Pagerange
            for j in range(len(Record_Location[i])):
                record = bytearray_to_record(self.PageRange_list[Record_PageRangeName[i]].base_pages,
                                             Record_Location[i][j], self.key_column)
                # print("record rid = ", record.rid)
                # print("record Indirection in base page = ", record.Indirection)
                # print("record Schema_Encoding = ", bin(record.Schema_Encoding))
                # print("record columns = ", record.columns)
                if record.rid == record.Indirection and record.rid == none:
                    print("find delete element!!")
                    continue
                record = Find_Newest_Record(record, self.PageRange_list[Record_PageRangeName[i]], self)
                Records.append(record)

            self.PageRange_list[Record_PageRangeName[i]].Evict = self.GobalEvict
		
        return Records


    def delete(self, key):
        Record_Location, Record_PageRangeName = self.index.locate(key, self.key_column + meta_data)
        self.GobalEvict = self.GobalEvict + 1
        NeedLoadPageRangeName = set()
        for i in range(len(Record_PageRangeName)):
            if Record_PageRangeName[i] in self.PageRange_list.keys():
                for j in range(len(Record_Location[i])):
                    record = bytearray_to_record(self.PageRange_list[Record_PageRangeName[i]].base_pages, Record_Location[i][j], self.key_column)
                    while record.Indirection != none:
                        Index_Tail_page = record.Indirection // 4096 + 1
                        Offset_Tail_page = record.Indirection % 4096
                        # setting all delete tail page record RID as none
                        self.PageRange_list[Record_PageRangeName[i]].tail_pages[Index_Tail_page].physical_pages[RIDCol].data[
                        Offset_Tail_page: Offset_Tail_page + ColSize] = none.to_bytes(8, "little")

                        record = TailPage_Record_From_RID(record.Indirection, self.PageRange_list[Record_PageRangeName[i]], self)
                        if record == None:
                            print("Error: didn't find anything to delete")
                            break
                    self.PageRange_list[Record_PageRangeName[i]].base_pages.physical_pages[IndirectionCol].offset_write(
                        Record_Location[i][j], none)
                    self.PageRange_list[Record_PageRangeName[i]].base_pages.physical_pages[RIDCol].offset_write(
                        Record_Location[i][j], none)

                self.PageRange_list[Record_PageRangeName[i]].Evict = self.GobalEvict
                self.PageRange_list[Record_PageRangeName[i]].dirty = True

            else:
                NeedLoadPageRangeName.add(i)


        for i in NeedLoadPageRangeName:
            self.Load_PageRange(Record_PageRangeName[i])
            for j in range(len(Record_Location[i])):
                record = bytearray_to_record(self.PageRange_list[Record_PageRangeName[i]].base_pages,
                                             Record_Location[i][j], self.key_column)
                while record.Indirection != none:
                    Index_Tail_page = record.Indirection // 4096 + 1
                    Offset_Tail_page = record.Indirection % 4096
                    # setting all delete tail page record RID as none
                    self.PageRange_list[Record_PageRangeName[i]].tail_pages[Index_Tail_page].physical_pages[
                        RIDCol].data[
                    Offset_Tail_page: Offset_Tail_page + ColSize] = none.to_bytes(8, "little")

                    record = TailPage_Record_From_RID(record.Indirection, self.PageRange_list[Record_PageRangeName[i]],
                                                      self)
                    if record == None:
                        print("Error: didn't find anything")
                        break
                self.PageRange_list[Record_PageRangeName[i]].base_pages.physical_pages[IndirectionCol].offset_write(
                    Record_Location[i][j], none)
                self.PageRange_list[Record_PageRangeName[i]].base_pages.physical_pages[RIDCol].offset_write(
                    Record_Location[i][j], none)

            self.PageRange_list[Record_PageRangeName[i]].Evict = self.GobalEvict
            self.PageRange_list[Record_PageRangeName[i]].dirty = True

        # for PRname in Record_PageRangeName:
        #     self.index.delete_key(self.key_column + meta_data, key, PRname)




    def update(self, key, *columns):
        # print("update", key, " column= ", columns)
        if len(self.PageRange_list) == 0:
            print("Error: Don't have page range at all!!!!")
            return False

        Record_Location, Record_PageRangeName = self.index.locate(key, self.key_column + meta_data)
        if len(Record_Location) == 0:
            print("Error: don't find the key in the Table")
            return False
        if len(Record_Location) > 1:
            print("Error: find multiple keys!!!!")
            return False
        elif len(Record_Location[0]) > 1:
            print("Error: find multiple keys!!!!")
            return False


        if Record_PageRangeName[0] not in self.PageRange_list.keys():
            self.Load_PageRange(Record_PageRangeName[0])

        self.PageRange_list[Record_PageRangeName[0]].Evict = self.GobalEvict

        Pagerange = self.PageRange_list[Record_PageRangeName[0]]
        ## find the Page Range have the key
        record = bytearray_to_record(Pagerange.base_pages, Record_Location[0][0], self.key_column)
        # print("page_range.tail_pages[page_range.current_tail_pages].physical_pages[RIDCol].num_entries", page_range.tail_pages[page_range.current_tail_pages].physical_pages[RIDCol].num_entries)
        RID = Pagerange.tail_pages[Pagerange.current_tail_pages].physical_pages[
                  RIDCol].num_entries * ColSize + PageSize * (
                          Pagerange.current_tail_pages - 1)  ##maybe need revise here
        # print("Rid in tail page is ", RID)
        Indirection = record.Indirection
        # print("record.Indirection is ", Indirection)


        temp = 0
        for i in columns:
            if i == None:
                temp = temp << 1
            else:
                temp = temp << 1 | 0b1
        Schema = temp
        # print("temp = ", bin(temp))
        Start_time = current_milli_time()
        # print("insert_record_to_tail RID = ", RID, "Indirection= ", Indirection, "Schema = ", bin(Schema), "columns = ", columns)
        Pagerange.insert_record_to_tail(RID, Indirection, Schema, Start_time, columns)

        Pagerange.base_pages.physical_pages[IndirectionCol].offset_write(Record_Location[0][0], RID)
        Pagerange.base_pages.physical_pages[SchemaCol].offset_write(Record_Location[0][0], record.Schema_Encoding | temp)
        self.GobalEvict = self.GobalEvict + 1
        Pagerange.dirty = True
        Pagerange.Evict = self.GobalEvict
        # self.index.update_index(record.columns, Page_range, Record_Location[0][0], *columns)
        return True


    def sum(self, start_range, end_range, aggregate_column_index):
        sum_value = 0
        self.GobalEvict = self.GobalEvict + 1
        NeedLoadPageRangeName = set()
        offsets = {}
        for key in range(start_range, end_range + 1):
            Record_offset, Record_PageRangeName = self.index.locate(key, self.key_column + meta_data)
            # print("Record_offset = ", Record_offset)
            # print("Record_PageRangeName = ", Record_offset)
            if len(Record_PageRangeName) == 1 and len(Record_offset[0]) == 1:
                if Record_PageRangeName[0] in self.PageRange_list.keys():
                    record = bytearray_to_record(self.PageRange_list[Record_PageRangeName[0]].base_pages, Record_offset[0][0], self.key_column)
                    if record.rid == record.Indirection and record.rid == none:
                        continue
                    record = Find_Newest_Record(record, self.PageRange_list[Record_PageRangeName[0]], self)
                    # print("find value with key =  ", key)

                    value = record.columns[aggregate_column_index]

                    # print("value =", value)
                    sum_value = sum_value + value
                    self.PageRange_list[Record_PageRangeName[0]].Evict = self.GobalEvict
                else:
                    NeedLoadPageRangeName.add(Record_PageRangeName[0])
                    if Record_PageRangeName[0] in offsets.keys():
                        offsets[Record_PageRangeName[0]].append(Record_offset[0][0])
                    else:
                        offsets[Record_PageRangeName[0]] = []
                        offsets[Record_PageRangeName[0]].append(Record_offset[0][0])
            elif len(Record_PageRangeName) == 0:
                # print("Error: sum don't find key!!!")
                pass
            else:
                print("Error: sum find multiple key!!!")

        for name in NeedLoadPageRangeName:
            # print("sum need load name ", name)
            self.Load_PageRange(name)
            for offset in offsets[name]:
                record = bytearray_to_record(self.PageRange_list[name].base_pages, offset,
                                             self.key_column)
                if record.rid == record.Indirection and record.rid == none:
                    continue
                record = Find_Newest_Record(record, self.PageRange_list[name], self)
                # print("find value with key =  ", key)

                value = record.columns[aggregate_column_index]

                # print("value =", value)
                self.PageRange_list[name].Evict = self.GobalEvict
                sum_value = sum_value + value
        ###!!!!!!! here!!!! not impelemented
        ## all the
        return sum_value


    def __write_base_page(self, record : Record, page_range: PageRange, offset : int):
        page_range.base_pages.physical_pages[RIDCol].offset_write(
            offset, record.rid)
        page_range.base_pages.physical_pages[IndirectionCol].offset_write(
            offset, record.Indirection)
        page_range.base_pages.physical_pages[SchemaCol].offset_write(
            offset,
            0)
        page_range.base_pages.physical_pages[TimestampCol].offset_write(
            offset,
            current_milli_time())
        for i in range(self.num_columns):
            page_range.base_pages.physical_pages[i + 4].offset_write(
                offset,
                record.columns[i])


    def merge(self, page_range : PageRange):

        if page_range.name not in self.index.indices[self.key_column + meta_data].DataFrame.keys():
            print("Error: don't find the page range when merge!!")
            exit(1)


        for key in self.index.indices[self.key_column + meta_data].DataFrame[page_range.name].keys():
            record = self.merge_select(key, self.key_column, [1]*self.num_columns)

            if len(record) > 1:
                print("Error:find multiple records with same key in merge!!!")
            if len(record) == 0:
                print("Error:don't find record with same key in merge!!!")


            record[0].Indirection = none
            # print("record rid = ", record[0].rid)
            # print("record rid = ", record[0].Indirection)
            # print("record rid = ", record[0].Schema_Encoding)
            # print("record rid = ", record[0].columns)

            offset = self.index.indices[self.key_column + meta_data].DataFrame[page_range.name][key]
            self.__write_base_page(record[0], page_range, offset[0])
        page_range.delete_tail_pages()




    def merge_select(self, key, column, query_columns): ##same as select but only difference is also return the deleted items
        if column + meta_data not in self.index.indices.keys():
            print("create new index")
            self.index.create_index(column + meta_data)

        Record_Location, Record_PageRangeName = self.index.locate(key, column + meta_data)
        if len(Record_Location) == 0:
            print("Error: don't find the key!!")
            exit(1)

        self.GobalEvict = self.GobalEvict + 1
        Records = []
        NeedLoadPageRangeName = set()
        for i in range(len(Record_PageRangeName)):
            if Record_PageRangeName[i] in self.PageRange_list.keys():
                for j in range(len(Record_Location[i])):
                    record = bytearray_to_record(self.PageRange_list[Record_PageRangeName[i]].base_pages, Record_Location[i][j], self.key_column)
                    # print("record rid = ", record.rid)
                    # print("record Indirection in base page = ", record.Indirection)
                    # print("record Schema_Encoding = ", bin(record.Schema_Encoding))
                    # print("record columns = ", record.columns)
                    if record.rid == record.Indirection and record.rid == none:
                        Records.append(record)
                    else:
                        record = Find_Newest_Record(record, self.PageRange_list[Record_PageRangeName[i]], self)
                        Records.append(record)


                self.PageRange_list[Record_PageRangeName[i]].Evict = self.GobalEvict
            else:
                NeedLoadPageRangeName.add(i)

        for i in NeedLoadPageRangeName:
            self.Load_PageRange(Record_PageRangeName[i])
            ## after insert current_pagerange is the new Pagerange
            for j in range(len(Record_Location[i])):
                record = bytearray_to_record(self.PageRange_list[Record_PageRangeName[i]].base_pages,
                                             Record_Location[i][j], self.key_column)
                # print("record rid = ", record.rid)
                # print("record Indirection in base page = ", record.Indirection)
                # print("record Schema_Encoding = ", bin(record.Schema_Encoding))
                # print("record columns = ", record.columns)
                if record.rid == record.Indirection and record.rid == none:
                    Records.append(record)
                else:
                    record = Find_Newest_Record(record, self.PageRange_list[Record_PageRangeName[i]], self)
                    Records.append(record)

            self.PageRange_list[Record_PageRangeName[i]].Evict = self.GobalEvict

        return Records
        
        
		def convert_key_to_RID(self,key,key_col):
			rids = []
			Record_Location, Record_PageRangeName = self.index.locate(key, key_column + meta_data)
			if key_col == self.key_column:
				#convert location to rid
				i = 0
				for name in self.PageRangename_Set:
					if name == Record_PageRangeName[0]:
						break
					i += 1
				rid = Record_Location[0] + i * PageSize
				rids.append(rid)
				return rids
			else:
				i = 0
				for pagerange in Record_PageRangeName:
					for name in self.PageRangename_Set
						if pagerange == name
							break
						i += 1
					rid = offset[i] + i * PageSize
					rids.append(rid)
				return rids
			
		def get_required_pageranges_from_queries(self,queries):
			ret_pageranges = []
			for query, args in self.queries:
				if query.__name__ == "insert"
				#if current page is not full, pin current page
				#else create new page range, pin the new pagerange
				if query.__name__ in ["delete", "update"]
					primary_key = args[0]
					Record_Location, Record_PageRangeName = self.index.locate(primary_key, self.key_column + meta_data)
						ret_pageranges.append(Record_PageRangeName[0])
				if query.__name__ == "select"
					index_key = args[0]
					key_column = args[1]
					Record_Location, Record_PageRangeName = self.index.locate(index_key, key_column + meta_data)
					ret_pageranges.extend(Record_PageRangeName)
			return ret_pageranges
			
			
		def precheck_locks_and_pin(self,queries):
			self.lock.acquire()
			rids = []
			required_pageranges = []
			ret_slock_rids = []
			ret_xlock_rids = []
			ret_pinned_pageranges = []
			for query, args in self.queries:
				if query.__name__ == "insert":
					# donnot need to check lock for insert
					# pin current page or pin the new page if the current page is full
				if query.__name__ == "sum":
					start_key = args[0]
					end_key = args[1]
					#loop through all keys to find accessing rids
					for i in range(start_key, end_key+1)
						rids = self.convert_key_to_RID(i, self.key_column)
						#acquire xlock
						if self.lock_manager.acquire_xlock(rids[0]) == False:
							return False
						else:
							rids.append(rids[0])
					
					#if xlock and be acquired, acquire slock
					for rid in rids:
						self.lock_manager.add_sLock(rid)
					ret_slock_rids = rids
					
				if query.__name__ == "select":
				key = args[0]
				key_col = args[1]
				rids = self.convert_key_to_RID(key, key_col)
				for rid in rids:
					retval = self.lock_manager.acquire_xlock(rid)
					if retval == False:
						return False
				#if xlock and be acquired, acquire slock
				for rid in rids:
					self.lock_manager.add_sLock(rid)
				ret_slock_rids.extend(rids)

				if query.__name__ == "delete":
					key = args[0]
					rids = self.convert_key_to_RID(key, self.key_column)
					#can only be deleted if no transaction is reading or writing to/from it
					if self.lock_manager.find_slock(rids[0]) == true and self.lock_manager.acquire_xlock(rids[0]) == true:
						pass
					else:
						return False
					ret_xlock_rids.append(rids[0])
				
					
				if query.__name__ == "update":
					key = args[0]
					rids = self.convert_key_to_RID(key, self.key_column)
					#cannot update if some transaction is currently reading or writing from/to the record
					if self.lock_manager.acquire_xlock(rids[0]) == False or self.lock_manager.find_slock(rids[0] == False):
						return False
					
					ret_xlock_rids.append(rids[0])
			required_pageranges = self.get_required_pageranges_from_queries(queries)
			self.lock.release()
		return ret_slock_rids,ret_xlock_rids, required_pageranges
		
		
		def release_locks(self,t_slocks, t_xlocks):
			self.lock.acquire()
			#release shared locks
			for slock in self.sLocks:
				for t_slock in t_slocks:
					if slock == t_slock:
						self.sLocks.remove(slock)
			#release exclusive locks
			for xlock in self.xLocks:
				for t_xlock in t_xlocks:
					if xlock == t_xlock:
						self.xLocks.remove(xlock)
			self.lock.release()
			
		def unpin_pages(self,unpinning_pages):
			for unpin_page in unpinning_pages:
				for page in self.PageRange_list:
					if page == unpin_page:
						index = self.PageRange_list.index(page)
						self.PageRange_list[index].pin -= 1
					
		
