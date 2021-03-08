from template.table import Table, Record
from template.index import Index

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs a transaction
    """
    def execute_func(transaction):
		# each transaction returns True if committed or False if aborted
		self.stats.append(transaction.run())
    
    def run(self):
        for transaction in self.transactions:
            t = threading.Thread(target = self.execute_func, args = (transaction,))
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

