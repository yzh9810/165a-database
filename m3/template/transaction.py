from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.table = None
        self.slock_rids = []
        self.xlock_rids = []
        self.required_pageranges = []
        self.acquire_lock_failure = False
        self.lock = threading.lock()
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))
        
        
	"""
    # check lock on records before executing queries. If a lock is detected, return false; If not, return true.
    """
	
			
	

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
		self.lock.acquire()
		self.table = self.queries[0][0].__self__.table
		lock_rids_and_pin = self.table.precheck_locks(self.queries)
		if lock_rids_and_pin == False:
			self.acquire_lock_failure = True
			abort()
		else:
			self.slock_rids = lock_rids_and_pin[0]
			self.xlock_rids = lock_rids_and_pin[1]
			self.required_pageranges = lock_rids_and_pin[2]
		#self.queries[0][0].__self__.table = self.table# update query table
		self.lock.release()
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
		self.lock.acquire()
        #TODO: do roll-back and any other necessary operations
        if self.acquire_lock_failure == True: # abort because of acquire lock failed during pre-check
			return False
		else # abort because query failed during execution
			# do not save the copy pages to disk
			self.table.unpin_pages(self.required_pageranges):
			self.table.release_locks(self.slock_rids, self.xlock_rids)
			
		self.lock.release()
        return False


    def commit(self):
        # TODO: commit to database
        self.lock.acquire()
        
        # save the copy pages to disk
        self.table.unpin_pages(self.required_pageranges):
        self.table.release_locks(self.slock_rids, self.xlock_rids)
        
        self.lock.release()
        return True

