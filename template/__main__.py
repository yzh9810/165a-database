# from template.db import Database
# # from template.query import Query
# # from time import process_time
# # from random import choice, randrange
# #
# # # Student Id and 4 grades
# # db = Database()
# # grades_table = db.create_table('Grades', 5, 0)
# # query = Query(grades_table)
# # keys = []
# # value = ord('a')
# # insert_time_0 = process_time()
# # for i in range(0, 20):
# #     query.insert(906659671 + i, value, 0, 0, 0)
# #     keys.append(906659671 + i)
# #     value = value+1
# # insert_time_1 = process_time()
# #
# # print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)
# #
# # # Measuring update Performance
# # update_cols = [
# #     [ord('e'), None, None, None, None],
# #     [None, ord('f'), None, None, None],
# #     [None, None, ord('g'), None, None],
# #     [None, None, None, ord('h'), None],
# #     [None, None, None, None, ord('i')],
# # ]
# #
# # for i in range(10):
# #     print()
# #
# # update_time_0 = process_time()
# # for i in range(0, 2):
# #     query.update(keys[0], *(update_cols[i]))
# # update_time_1 = process_time()
# #
# # print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)
# #
# # for i in range(10):
# #     print()
# #
# # # # Measuring Select Performance
# # select_time_0 = process_time()
# # for i in range(0, 3):
# #     query.select(keys[i],0 , [1, 1, 1, 1, 1])
# #     print()
# # select_time_1 = process_time()
# # print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)
# #
# # # # Measuring Aggregate Performance
# # # agg_time_0 = process_time()
# # # for i in range(0, 10000, 100):
# # #     result = query.sum(i, 100, randrange(0, 5))
# # # agg_time_1 = process_time()
# # # print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)
# # #
# # # # Measuring Delete Performance
# # # delete_time_0 = process_time()
# # # for i in range(0, 10000):
# # #     query.delete(906659671 + i)
# # # delete_time_1 = process_time()
# # # print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)

from template.db import Database
from template.query import Query
from template.config import *

from random import choice, randint, sample, seed
from colorama import Fore, Back, Style

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}

seed(3562901)

for i in range(0, 1000):
    key = 92106429 + randint(0, 9000)
    while key in records:
        key = 92106429 + randint(0, 9000)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    print('inserted', records[key])

for key in records:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        print('select on', key, ':', record)

for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(1, grades_table.num_columns):
        value = randint(0, 20)
        updated_columns[i] = value
        original = records[key].copy()
        records[key][i] = value
        # print("update on ", key, " updated_columns = ", updated_columns)
        query.update(key, *updated_columns)
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
        else:
            print('update on', original, 'and', updated_columns, ':', record)

        # for abc in range(1):
        #     print()
        updated_columns[i] = None

keys = sorted(list(records.keys()))
# print("keys = ", keys)
for c in range(0, grades_table.num_columns):
    # print(c)
    for i in range(0, 20):
        r = sorted(sample(range(0, len(keys)), 2))
        # print("r = ", r)
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        # print("column sum = ", column_sum)
        result = query.sum(keys[r[0]], keys[r[1]], c)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        else:
            print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)