import sqlite3
import csv
import time
import argparse
from eventTypes import dcEventTypes


conn = sqlite3.connect( 'C:\\xampp\htdocs\www\ps\db\AHSMap.sqlite' )
cur = conn.cursor()

missing_rooms = { }

def get_room_index(room_number):
    cur.execute('SELECT id FROM Room WHERE ? IN (room_num, old_num)', (room_number,))

    try:
        # Try to get room from database
        room_index = cur.fetchone()[0]
    except:
        # Track missing room
        missing_rooms[room_number] = room_number

        # Work around missing room by adding it to the database
        cur.execute('''INSERT OR IGNORE INTO Room (room_num, old_num, location_type, description)
                    VALUES (?,?,?,? )''', (room_number, room_number, room_number, room_number))
        conn.commit()

        # Retry
        room_index = get_room_index(room_number)

    return room_index

def get_circuit_index(circuit_path):
    cur.execute('SELECT id FROM CircuitObject WHERE path = ?', (circuit_path,))
    index = cur.fetchone()
    return index[0]

def get_voltage_index(voltage):
    print(voltage)
    cur.execute('SELECT id FROM Voltage WHERE ? IN (description)', (voltage,))
    index = cur.fetchone()
    return index[0]


def make_database( bDestroy ):

    # Optionally destroy history of users and activity
    if bDestroy:
        cur.executescript( '''
            DROP TABLE IF EXISTS User;
            DROP TABLE IF EXISTS Activity;
        ''' )

    #Builds SQLite database
    cur.executescript('''

        DROP TABLE IF EXISTS Room;
        DROP TABLE IF EXISTS CircuitObject;
        DROP TABLE IF EXISTS Device;
        DROP TABLE IF EXISTS Voltage;

        CREATE TABLE IF NOT EXISTS User (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            username TEXT UNIQUE,
            password TEXT,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS Activity (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            timestamp FLOAT,
            username TEXT,
            event_type TEXT,
            target_table TEXT,
            target_column TEXT,
            target_value TEXT,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS Room (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        room_num TEXT,
        old_num TEXT,
        location_type TEXT,
        description TEXT
        );

        CREATE TABLE IF NOT EXISTS CircuitObject (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        room_id INTEGER,
        path TEXT,
        zone TEXT,
        voltage_id TEXT,
        object_type TEXT,
        description TEXT,
        parent TEXT,
        tail TEXT
        );

        CREATE TABLE IF NOT EXISTS Device (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        room_id INTEGER,
        panel_id INTEGER,
        description TEXT,
        power TEXT,
        parent TEXT
        );

        CREATE TABLE IF NOT EXISTS Voltage (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        description TEXT UNIQUE
        )


    ''')

    cur.execute( '''INSERT OR IGNORE INTO User ( username, password, description ) VALUES (?,?,? )''', ('system', '', 'system') )

    cur.execute('''INSERT INTO Activity ( timestamp, username, event_type, target_table, target_column, target_value, description )
        VALUES (?,?,?,?,?,?,? )''', ( time.time(), 'system', dcEventTypes['database'], '', '', '', 'Start generating database from CSV files' ) )

    conn.commit()


    # builds Room table
    with open('rooms.csv','r') as f:
        readfile = csv.reader(f)
        for line in readfile:
            print(line)
            if (line[0] == 'Old Number'):
                continue

            old_num = line[0]
            new_num = line[1]
            description = line[2]
            loc_type = 'no data'

            if old_num == '':
                old_num = 'no old room'

            if new_num == '':
                new_num = 'no new room'

            if description == '':
                description = 'no description'

            #print(new_num,old_num,description,loc_type)

            cur.execute('''INSERT OR IGNORE INTO Room (room_num, old_num, location_type, description)
                VALUES (?,?,?,? )''', (new_num, old_num, loc_type, description) )

            conn.commit()




    with open('pathways.csv','r') as file:
        circuitreader = csv.reader(file)

        for line in circuitreader:
            if line[0] == 'path' or line[0] == '':
                continue
            #print('get room index for room', line[3])
            #print('voltage is ', line[2])
            voltage = line[2]
            cur.execute('''INSERT OR IGNORE INTO Voltage (description) VALUES (?)''', (voltage,))
            #conn.commit()
            roomid = get_room_index(line[3])
            zone = 'unknown'

            volt_id = get_voltage_index(voltage)
            path = line[0]
            objectType = line[1]
            desc = line[4]

            tail = path.split('.')[-1]
            if tail.isdigit():
              tail = ''

            parent = path.rsplit('.',maxsplit=1)[0]
            print(parent)
            if parent == path:
                parent = ''

            #print('inserting', path, roomid, zone, volt_id, objectType, desc)
            cur.execute('''INSERT OR IGNORE INTO CircuitObject (path, room_id, zone, voltage_id, object_type, description, parent, tail )
                VALUES (?,?,?,?,?,?,?,?)''', (path, roomid, zone, volt_id, objectType, desc, parent, tail))

            conn.commit()



    with open ('devices.csv', 'r') as file:
        devicereader = csv.reader(file)

        for line in devicereader:
            print(line)

            if line[0] == 'DeviceObj':
                continue

            #if room is unknown, insert unknown as room
            if line[2] == '':
                roomid = 'UNKNOWN'
            else:
                roomid = get_room_index(line[2])

            panelid = get_circuit_index(line[1])
            description = line[0]
            parent = line[1]

            print(roomid, panelid, description, parent)
            cur.execute('''INSERT OR IGNORE INTO Device (room_id, panel_id, description, parent)
                 VALUES (?,?,?,?)''', (roomid, panelid, description, parent))

            conn.commit()


    cur.execute('''INSERT INTO Activity ( timestamp, username, event_type, target_table, target_column, target_value, description )
        VALUES (?,?,?,?,?,?,? )''', ( time.time(), 'system', dcEventTypes['database'], '', '', '', 'Finished generating database from CSV files' ) )

    conn.commit()


    # Dump referenced rooms that are missing from rooms.csv
    missing_keys = sorted( missing_rooms.keys() )
    if ( len( missing_keys ) > 0 ):
        print( '' )
        print( '' )
        print( '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' )
        print( 'ROOMS LISTED BELOW ARE MISSING FROM rooms.csv' )
        print( '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' )
        print( '' )
        for missing in iter( missing_keys ):
            print( missing )
        print( '' )
        print( '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' )
        print( 'ROOMS LISTED ABOVE ARE MISSING FROM rooms.csv' )
        print( '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' )
        print( '' )


# Main program
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='script to generate database')
    parser.add_argument('-d', dest='destroy', action='store_true', help='destroy history')
    args = parser.parse_args()

    make_database( args.destroy )
