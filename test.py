from schema import db

search_dict = {"LastName": "Cameron"}

camerons = db['agent'].find(search_dict)

if len(camerons) > 0:
    print('we have the following camerons:')
    for agent in camerons:
        print(agent.FirstName)
else:
    print('there are no camerons...')