from db.db import get_db

db = get_db('root', 'root', 'localhost', 'specify_php', 'arachnida')

search_dict = {"FullName": "Opistophthalmus"}

try:
    results = db.taxa.find(search_dict)
except Exception as ex:
    db.close()
    print(str(ex))
    exit()

if len(results) > 0:
    print('we have the following results:')
    for result in results:
        print(result)
else:
    print('there are no results...')

db.close()