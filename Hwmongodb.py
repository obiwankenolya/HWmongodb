import csv
import re
from pymongo import MongoClient


client = MongoClient()
obiwankenolya_db = client['obiwankenolya']

date_list = []
performer_list = []
price_list = []
place_list = []


def read_data(csv_file, db):
    concerts_collection = db['concerts']
    with open(csv_file, encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile)
        for key in reader:
            for item in key:
                date_list.append(key[item])
        for item in date_list:
            if date_list.index(item) % 4 == 0:
                performer_list.append(item)
        for item in performer_list:
            if item in date_list:
                date_list.remove(item)
        for item in date_list:
            if date_list.index(item) % 3 == 0:
                price_list.append(item)
        for item in price_list:
            if item in date_list:
                date_list.remove(item)
        for item in date_list:
            if date_list.index(item) % 2 == 0:
                place_list.append(item)
        for item in place_list:
            if item in date_list:
                date_list.remove(item)
        for performer in performer_list:
            i = performer_list.index(performer)
            concert = {
                'performer': performer,
                'price': int(price_list[i]),
                'place': place_list[i],
                'date': date_list[i]
            }
            yield concerts_collection.insert_one(concert).inserted_id


def find_cheapest(db):
    read_data('artists.csv', obiwankenolya_db)
    return list(db.concerts_collection.find().sort({'price': -1}))


def find_by_name(name, db):
    read_data('artists.csv', obiwankenolya_db)
    regex = re.compile(name, re.I)
    return list(db.concerts_collection.find({'performer': regex}).sort({'price': 1}))


if __name__ == '__main__':
    find_by_name('Lil\sJon', obiwankenolya_db)
