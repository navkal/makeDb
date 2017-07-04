import sys
sys.path.insert(0, 'C:\\xampp/htdocs/www/oops/database')

import sqlite3
import csv
import time
import json
import argparse
import dbCommon
import os

conn = None
cur = None


missing_rooms = { }

def get_room_index(room_number,sFacility=''):
    cur.execute('SELECT id FROM '+sFacility+'Room WHERE ? IN (room_num, old_num)', (room_number,))

    try:
        # Try to get room from database
        room_index = cur.fetchone()[0]
    except:
        # Track missing room
        if sFacility == '':
            sMissing = room_number
        else:
            sMissing = sFacility + ': ' + room_number
        missing_rooms[sMissing] = sMissing

        # Work around missing room by adding it to the database
        cur.execute('''INSERT OR IGNORE INTO '''+sFacility+'''Room (room_num, old_num, location_type, description)
                    VALUES (?,?,?,? )''', (room_number, room_number, '', room_number))
        conn.commit()

        # Retry
        room_index = get_room_index(room_number,sFacility)

    return room_index

def path_to_id(circuit_path, sFacility='' ):
    cur.execute('SELECT id FROM ' + sFacility + 'CircuitObject WHERE path = ?', (circuit_path,))
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



def make_database( sEnterprise, sFacilitiesCsv ):

    #Builds SQLite database
    cur.executescript('''

        CREATE TABLE IF NOT EXISTS Facility (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            facility_name TEXT UNIQUE,
            description TEXT
        );

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
            organization TEXT,
            facility_ids TEXT
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
            description TEXT,
            facility_id INTEGER
        );

        CREATE TABLE IF NOT EXISTS Voltage (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        description TEXT UNIQUE
        );
    ''')


    # Initialize roles
    cur.execute( '''INSERT OR IGNORE INTO Role ( role ) VALUES (?)''', ('Administrator',) )
    cur.execute( '''INSERT OR IGNORE INTO Role ( role ) VALUES (?)''', ('Technician',) )
    cur.execute( '''INSERT OR IGNORE INTO Role ( role ) VALUES (?)''', ('Visitor',) )
    conn.commit()

    # Initialize Facilities table
    aFacilities = []

    with open( sFacilitiesCsv,'r') as f:
        lines = csv.reader(f)
        for line in lines:
            if (line[0] == 'Name'):
                continue

            print( 'facility: ', line )
            facility_name = line[0].strip()
            description = line[1].strip()
            aFacilities.append( facility_name )

            cur.execute( 'INSERT OR IGNORE INTO Facility (facility_name, description) VALUES (?,?)', (facility_name, description) )

    conn.commit()

    # Retrieve all facility IDs
    cur.execute( 'SELECT id FROM Facility')
    rows = cur.fetchall()
    facility_id_list = []
    for row in rows:
        facility_id_list.append( str( row[0] ) )
    facility_id_csv = ",".join( facility_id_list )

    # Initialize default users
    cur.execute( '''INSERT OR IGNORE INTO User ( username, password, role_id, description ) VALUES (?,?,?,? )''', ('system', None, None, 'system') )

    if sEnterprise == 'demo':
        dbCommon.add_interactive_user( cur, conn, 'system', 'demo', 'demo', 'Visitor', False, True, 'Big', 'Bird', 'nest@sesame.com', 'Sesame Street', 'Demo User', facility_id_csv )
    else:
        dbCommon.add_interactive_user( cur, conn, 'system', 'admin', 'admin', 'Administrator', False, True, 'Oscar', 'Grouch', 'trash@sesame.com', 'Sesame Street', 'Administrator', '' )
        dbCommon.add_interactive_user( cur, conn, 'system', 'tech', 'tech', 'Technician', False, True, 'Cookie', 'Monster', 'oatmeal@sesame.com', 'Sesame Street', 'Default Technician', facility_id_csv )
        dbCommon.add_interactive_user( cur, conn, 'system', 'test', 'test', 'Visitor', False, True, 'Kermit', 'Frog', 'green@sesame.com', 'Sesame Street', 'Default Visitor', facility_id_csv )

    cur.execute('''INSERT INTO Activity ( timestamp, username, event_type, target_table, target_column, target_value, description )
        VALUES (?,?,?,?,?,?,? )''', ( time.time(), 'system', dbCommon.dcEventTypes['database'], '', '', '', 'Start generating tables from CSV files' ) )

    conn.commit()


    for sFacility in aFacilities:

        cur.executescript('''

            CREATE TABLE IF NOT EXISTS ''' + sFacility + '''Room (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            room_num TEXT,
            old_num TEXT,
            location_type TEXT,
            description TEXT
            );

            CREATE TABLE IF NOT EXISTS ''' + sFacility + '''CircuitObject (
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

            CREATE TABLE IF NOT EXISTS ''' + sFacility + '''Device (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            room_id INTEGER,
            parent_id INTEGER,
            description TEXT,
            power TEXT,
            name TEXT
            );

        ''')


        # builds Room table
        with open( sFacility + '_rooms.csv','r') as f:
            readfile = csv.reader(f)
            for line in readfile:
                print( sFacility + ' rooms', line )
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

                cur.execute('''INSERT OR IGNORE INTO ''' + sFacility + '''Room (room_num, old_num, location_type, description)
                    VALUES (?,?,?,? )''', (new_num, old_num, '', description) )

                conn.commit()



        # Create empty tree map
        tree_map = {}

        with open(sFacility + '_pathways.csv','r') as file:
            circuitreader = csv.reader(file)

            for line in circuitreader:
                print( sFacility + ' pathways', line )
                path = line[0].strip()
                if path == 'path' or path == '':
                    continue

                #print('get room index for room', line[3])
                #print('voltage is ', line[2])
                voltage = line[2].strip()
                cur.execute('''INSERT OR IGNORE INTO Voltage (description) VALUES (?)''', (voltage,))
                #conn.commit()
                roomid = get_room_index(line[3].strip(),sFacility)
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
                cur.execute('''SELECT room_num, old_num, description FROM ''' + sFacility + '''Room WHERE id = ?''', (roomid,))
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


                cur.execute('''INSERT OR IGNORE INTO ''' + sFacility + '''CircuitObject (path, room_id, zone, voltage_id, object_type, description, tail, search_text, source )
                    VALUES (?,?,?,?,?,?,?,?,?)''', (path, roomid, zone, volt_id, objectType, desc, tail, search_text, source))

                # Add node to tree map
                tree_map[path] = { 'name': path.rsplit( '.' )[-1], 'children': [] }

                conn.commit()


        # Load parent ID in table and parent-child relationship in tree map
        cur.execute( 'SELECT id, path FROM ' + sFacility + 'CircuitObject' )
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

                cur.execute( 'SELECT id FROM ' + sFacility + 'CircuitObject WHERE path = ?', (parent_path,) )
                parent_row = cur.fetchone()
                parent_id = parent_row[0]
                cur.execute( 'UPDATE ' + sFacility + 'CircuitObject SET parent_id = ? WHERE id= ?', (parent_id,row_id) )

        conn.commit()



        with open ( sFacility + '_devices.csv', 'r') as file:
            devicereader = csv.reader(file)

            for line in devicereader:
                print( sFacility + ' devices', line )

                name = line[0].strip()
                if name == 'DeviceObj':
                    continue

                if not name:
                  name = '?'

                parentid = path_to_id(line[1], sFacility )

                loc = line[2]
                if loc == '':
                    roomid = ''
                    location = ''
                    location_old = ''
                    location_descr = ''
                else:
                    roomid = get_room_index( loc, sFacility )
                    cur.execute('''SELECT room_num, old_num, description FROM ''' + sFacility + '''Room WHERE id = ?''', (roomid,))
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

                cur.execute('''INSERT OR IGNORE INTO ''' + sFacility + '''Device (room_id, parent_id, description, name)
                     VALUES (?,?,?,?)''', (roomid, parentid, desc, name))

                conn.commit()


        # Link devices into tree map
        cur.execute( 'SELECT id, parent_id, name FROM ' + sFacility + 'Device' )
        rows = cur.fetchall()

        for row in rows:
            row_id = row[0]
            parent_id = row[1]
            row_name = row[2]

            cur.execute( 'SELECT path FROM ' + sFacility + 'CircuitObject WHERE id = ?', (parent_id,) )
            parent_path = cur.fetchone()[0]
            row_path = parent_path + '.' + str( row_id )

            # Insert node in tree map and link to parent
            tree_map[row_path] = { 'name': row_name, 'children': [] }
            tree_map[parent_path]['children'] += [ tree_map[row_path] ]

        # Save tree map in JSON format
        with open( 'C:\\xampp/htdocs/www/oops/database/' + sEnterprise + '/' + sFacility + '/tree.json', 'w' ) as outfile:
            json.dump( tree_map[tree_map_root_path], outfile )


    cur.execute('''INSERT INTO Activity ( timestamp, username, event_type, target_table, target_column, target_value, description )
        VALUES (?,?,?,?,?,?,? )''', ( time.time(), 'system', dbCommon.dcEventTypes['database'], '', '', '', 'Finish generating tables from CSV files' ) )

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
    parser.add_argument('-e', dest='enterprise', help='enterprise' )
    parser.add_argument('-f', dest='facilities', help='comma-separated list of facility names' )
    args = parser.parse_args()

    # Create new empty databse
    sDbPath = 'C:/xampp/htdocs/www/oops/database/' + args.enterprise + '/database.sqlite'

    if os.path.exists( sDbPath ):
        os.remove( sDbPath )

    conn = sqlite3.connect( sDbPath )
    cur = conn.cursor()

    # Fill the database
    make_database( args.enterprise, args.facilities )
