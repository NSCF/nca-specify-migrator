from db.db import get_db

db = get_db('root', 'root', 'localhost', 'specify_php', 'arachnida')

search_dict = {"FullName": "Opistophthalmus"}

results = db.taxon.find(search_dict)

if len(results) > 0:
    print('we have the following results:')
    for result in results:
        print(result)
else:
    print('there are no results...')