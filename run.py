import json
import time
import urllib

from db import DB

URL = 'http://poketweet.herokuapp.com/raw_data'
DATABASE = 'poketweet.db'
TABLE_NAME = 'pokemon'

COLUMN_NAMES = ['encounter_id', 'pokemon_id', 'spawnpoint_id', 'latitude',
                'longitude', 'encounter_time', 'disappear_time']
COLUMN_TYPES = ['TEXT UNIQUE', 'INT', 'TEXT', 'NUM', 'NUM', 'NUM', 'NUM']

db = DB(filename=DATABASE, dbname=TABLE_NAME, dbtype='sqlite')
db.cur.execute('CREATE TABLE IF NOT EXISTS {}({});'.format(
    TABLE_NAME, ', '.join([' '.join(i) for i in zip(COLUMN_NAMES,
                                                    COLUMN_TYPES)])))


def db_refresh():
    global db
    db = DB(filename=DATABASE, dbname=TABLE_NAME, dbtype='sqlite')


def get_pokemons():
    r = urllib.urlopen(URL)
    data = json.load(r)

    return data['pokemons']


def add_entry(pokemon):
    entry = ['"{}"'.format(pokemon[column]) if type.startswith('TEXT')
             else str(pokemon[column])
             for column, type in zip(COLUMN_NAMES, COLUMN_TYPES)]
    db.cur.execute('INSERT OR IGNORE INTO {}({}) VALUES ({});'.format(
        TABLE_NAME, ', '.join(COLUMN_NAMES), ', '.join(entry)))


def main():
    db_refresh()
    while True:
        pokemons = get_pokemons()
        for pokemon in pokemons:
            add_entry(pokemon)
        time.sleep(30)


if __name__ == '__main__':
    main()
