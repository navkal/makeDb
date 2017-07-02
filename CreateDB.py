import sys
sys.path.insert(0, 'C:\\xampp/htdocs/www/oops/database')

import sqlite3
import csv
import time
import json
import argparse
import dbCommon
import os

sEnterprise = None
aFacilities = None

conn = None
cur = None


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
                    VALUES (?,?,?,? )''', (room_number, room_number, '', room_number))
        conn.commit()

        # Retry
        room_index = get_room_index(room_number)

    return room_index

def path_to_id(circuit_path):
    cur.execute('SELECT id FROM CircuitObject WHERE path = ?', (circuit_path,))
    index = cur.fetchone()
    return index[0]

def get_voltage_index(voltage):
    cur.execute('SELECT id FROM Voltage WHERE ? IN (description)', (voltage,))
    index = cur.fetchone()
    return index[0]

def append_location( desc, location, location_old, location_descr, end_delimiter ):

    if location or location_old or location_descr:

        if location:
            desc += ' ' + location

        if location_old:
            desc += ' (' + location_old + ')'

        if location_descr:
            desc += " '" + location_descr + "'"

        desc += end_delimiter

    return desc



def make_database():

    #Builds SQLite database
    cur.executescript('''

        DROP TABLE IF EXISTS User;
        DROP TABLE IF EXISTS Activity;
        DROP TABLE IF EXISTS Role;
        DROP TABLE IF EXISTS Room;
        DROP TABLE IF EXISTS CircuitObject;
        DROP TABLE IF EXISTS Device;
        DROP TABLE IF EXISTS Voltage;

        CREATE TABLE IF NOT EXISTS User (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            username TEXT UNIQUE,
            password TEXT,
            role_id INTEGER,
            description TEXT,
            force_change_password BOOLEAN,
            enabled BOOLEAN,
            first_name TEXT,
            last_name TEXT,
            email_address TEXT,
            organization TEXT
        );

        CREATE TABLE IF NOT EXISTS Role (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            role TEXT UNIQUE
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
        voltage_id INTEGER,
        object_type TEXT,
        description TEXT,
        parent_id INTEGER,
        tail TEXT,
        search_text TEXT,
        source TEXT
        );

        CREATE TABLE IF NOT EXISTS Device (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        room_id INTEGER,
        parent_id INTEGER,
        description TEXT,
        power TEXT,
        name TEXT
        );

        CREATE TABLE IF NOT EXISTS Voltage (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        description TEXT UNIQUE
        )

    ''')

    # Initialize roles
    cur.execute( '''INSERT OR IGNORE INTO Role ( role ) VALUES (?)''', ('Administrator',) )
    cur.execute( '''INSERT OR IGNORE INTO Role ( role ) VALUES (?)''', ('Technician',) )
    cur.execute( '''INSERT OR IGNORE INTO Role ( role ) VALUES (?)''', ('Visitor',) )
    conn.commit()

    # Initialize default users
    cur.execute( '''INSERT OR IGNORE INTO User ( username, password, role_id, description ) VALUES (?,?,?,? )''', ('system', None, None, 'system') )
    dbCommon.add_interactive_user( cur, conn, 'system', 'admin', 'admin', 'Administrator', False, True, 'Oscar', 'Grouch', 'trash@sesame.com', 'Sesame Street', 'Administrator' )
    dbCommon.add_interactive_user( cur, conn, 'system', 'tech', 'tech', 'Technician', False, True, 'Cookie', 'Monster', 'oatmeal@sesame.com', 'Sesame Street', 'Default Technician' )
    dbCommon.add_interactive_user( cur, conn, 'system', 'test', 'test', 'Visitor', False, True, 'Kermit', 'Frog', 'green@sesame.com', 'Sesame Street', 'Default Visitor' )
    dbCommon.add_interactive_user( cur, conn, 'system', 'demo', 'demo', 'Visitor', False, True, 'Big', 'Bird', 'nest@sesame.com', 'Sesame Street', 'Demo User' )

    cur.execute('''INSERT INTO Activity ( timestamp, username, event_type, target_table, target_column, target_value, description )
        VALUES (?,?,?,?,?,?,? )''', ( time.time(), 'system', dbCommon.dcEventTypes['database'], '', '', '', 'Start generating database from CSV files' ) )

    conn.commit()


    # builds Room table
    with open('rooms.csv','r') as f:
        readfile = csv.reader(f)
        for line in readfile:
            print( 'rooms', line )
            if (line[0] == 'Old Number'):
                continue

            old_num = line[0].strip()
            new_num = line[1].strip()
            description = line[2].strip()

            if old_num.startswith( 'UNKNOWN' ):
                old_num = '?'
            if new_num.startswith( 'UNKNOWN' ):
                new_num = '?'
            if description.startswith( 'UNKNOWN' ):
                description = '?'

            #print(new_num,old_num,description,loc_type)

            cur.execute('''INSERT OR IGNORE INTO Room (room_num, old_num, location_type, description)
                VALUES (?,?,?,? )''', (new_num, old_num, '', description) )

            conn.commit()


    # Create empty tree map
    tree_map = {}

    with open('pathways.csv','r') as file:
        circuitreader = csv.reader(file)

        for line in circuitreader:
            print( 'pathways', line )
            path = line[0].strip()
            if path == 'path' or path == '':
                continue

            #print('get room index for room', line[3])
            #print('voltage is ', line[2])
            voltage = line[2].strip()
            cur.execute('''INSERT OR IGNORE INTO Voltage (description) VALUES (?)''', (voltage,))
            #conn.commit()
            roomid = get_room_index(line[3].strip())
            zone = 'unknown'

            volt_id = get_voltage_index(voltage)
            objectType = line[1].strip()

            # Initialize path and path fragments
            pathsplit = path.split('.')
            name = pathsplit[-1]

            tail = name
            if tail.isdigit():
              tail = ''

            if len( pathsplit ) == 1:
              source = ''
            else:
              source = pathsplit[-2]

            # Initialize description fragments
            cur.execute('''SELECT room_num, old_num, description FROM Room WHERE id = ?''', (roomid,))
            rooms = cur.fetchone()
            location = rooms[0]
            location_old = rooms[1]
            location_descr = rooms[2]
            bar = ' | '

            if objectType.lower() == 'panel':
                # It's a panel; generate description
                search_text = ''
                desc = ''
                if source:
                    desc += ' ' + source + bar
                if voltage:
                    desc += ' ' + voltage + 'V' + bar

                desc = append_location( desc, location, location_old, location_descr, bar )

                if desc:
                    desc = desc[:-3]
            else:
                # Not a panel; use description field from CSV file
                search_text = line[4].strip()
                desc = ' ' + search_text

            if desc.strip():
                desc = name + ':' + desc
            else:
                desc = name


            cur.execute('''INSERT OR IGNORE INTO CircuitObject (path, room_id, zone, voltage_id, object_type, description, tail, search_text, source )
                VALUES (?,?,?,?,?,?,?,?,?)''', (path, roomid, zone, volt_id, objectType, desc, tail, search_text, source))

            # Add node to tree map
            tree_map[path] = { 'name': path.rsplit( '.' )[-1], 'children': [] }

            conn.commit()

    # Load parent ID in table and parent-child relationship in tree map
    cur.execute( 'SELECT id, path FROM CircuitObject' )
    rows = cur.fetchall()

    tree_map_root_path = ''

    for row in rows:
        row_id = row[0]
        row_path = row[1]
        parent_path = row_path.rsplit( '.', maxsplit=1 )[0]

        if parent_path == row_path:
            # Save root path
            tree_map_root_path = row_path
        else:
            # Link current node to its parent in tree map
            tree_map[parent_path]['children'] += [ tree_map[row_path] ]

            cur.execute( 'SELECT id FROM CircuitObject WHERE path = ?', (parent_path,) )
            parent_row = cur.fetchone()
            parent_id = parent_row[0]
            cur.execute( 'UPDATE CircuitObject SET parent_id = ? WHERE id= ?', (parent_id,row_id) )

    conn.commit()


    with open ('devices.csv', 'r') as file:
        devicereader = csv.reader(file)

        for line in devicereader:
            print( 'devices', line )

            name = line[0].strip()
            if name == 'DeviceObj':
                continue

            if not name:
              name = '?'

            parentid = path_to_id(line[1])

            loc = line[2]
            if loc == '':
                roomid = ''
                location = ''
                location_old = ''
                location_descr = ''
            else:
                roomid = get_room_index(loc)
                cur.execute('''SELECT room_num, old_num, description FROM Room WHERE id = ?''', (roomid,))
                rooms = cur.fetchone()
                location = rooms[0]
                location_old = rooms[1]
                location_descr = rooms[2]

            # Generate description
            desc = append_location( '', location, location_old, location_descr, '' )
            if desc:
                desc = name + ':' + desc
            else:
                desc = name

            cur.execute('''INSERT OR IGNORE INTO Device (room_id, parent_id, description, name)
                 VALUES (?,?,?,?)''', (roomid, parentid, desc, name))

            conn.commit()


    cur.execute('''INSERT INTO Activity ( timestamp, username, event_type, target_table, target_column, target_value, description )
        VALUES (?,?,?,?,?,?,? )''', ( time.time(), 'system', dbCommon.dcEventTypes['database'], '', '', '', 'Finish generating database from CSV files' ) )

    conn.commit()


    # Link devices into tree map
    cur.execute( 'SELECT id, parent_id, name FROM Device' )
    rows = cur.fetchall()

    for row in rows:
        row_id = row[0]
        parent_id = row[1]
        row_name = row[2]

        cur.execute( 'SELECT path FROM CircuitObject WHERE id = ?', (parent_id,) )
        parent_path = cur.fetchone()[0]
        row_path = parent_path + '.' + str( row_id )

        # Insert node in tree map and link to parent
        tree_map[row_path] = { 'name': row_name, 'children': [] }
        tree_map[parent_path]['children'] += [ tree_map[row_path] ]



    # Save tree map in JSON format
    with open( 'C:\\xampp/htdocs/www/oops/database/andover/ahs/tree.json', 'w' ) as outfile:
        json.dump( tree_map[tree_map_root_path], outfile )


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
    parser.add_argument('-e', dest='enterprise', help='enterprise' )
    parser.add_argument('-f', dest='facilities', help='comma-separated list of facility names' )
    args = parser.parse_args()

    sEnterprise = args.enterprise
    aFacilities = args.facilities.split( ',' )
    sDbPath = 'C:/xampp/htdocs/www/oops/database/' + sEnterprise + '/database.sqlite'

    if os.path.exists( sDbPath ):
        os.remove( sDbPath )

    conn = sqlite3.connect( sDbPath )
    cur = conn.cursor()

    make_database()
