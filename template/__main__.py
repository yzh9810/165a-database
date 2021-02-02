from template.db import Database
from template.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []
value = ord('a')
insert_time_0 = process_time()
for i in range(0, 10000):
    query.insert(906659671 + i, value, 0, 0, 0)
    keys.append(906659671 + i)
    value = value+1
insert_time_1 = process_time()

print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measuring update Performance
update_cols = [
    [ord('e'), None, None, None, None],
    [None, ord('f'), None, None, None],
    [None, None, ord('g'), None, None],
    [None, None, None, ord('h'), None],
    [None, None, None, None, ord('i')],
]



update_time_0 = process_time()
for i in range(0, 10000):
    query.update(keys[i], *(choice(update_cols)))
update_time_1 = process_time()

print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)

# for i in range(10):
#     print()

# # Measuring Select Performance
select_time_0 = process_time()
for i in range(0, 10000):
    query.select(keys[i],0 , [1, 1, 1, 1, 1])
select_time_1 = process_time()
print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)

# Measuring Aggregate Performance
agg_time_0 = process_time()
for i in range(0, 10000, 100):
    result = query.sum(i, 100, randrange(0, 5))
agg_time_1 = process_time()
print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)

# Measuring Delete Performance
delete_time_0 = process_time()
for i in range(0, 10000):
    query.delete(906659671 + i)
delete_time_1 = process_time()
print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)
