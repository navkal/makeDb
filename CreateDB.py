import sys
sys.path.insert(0, 'E:\\xampp/htdocs/www/oops/database')

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

def append_location( text, location, location_old, location_descr, end_delimiter ):

    if location or location_old or location_descr:

        if location:
            text += ' ' + location

        if location_old:
            text += ' (' + location_old + ')'

        if location_descr:
            text += " '" + location_descr + "'"

        text += end_delimiter

    return text




def make_room_table( sFacility ):

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




def make_circuit_object_table( sFacility ):

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
            zone = ''

            volt_id = get_voltage_index(voltage)
            objectType = line[1].strip().title()

            # Initialize path and path fragments
            pathsplit = path.split('.')
            name = pathsplit[-1]

            if len( pathsplit ) == 1:
              source = ''
            else:
              source = pathsplit[-2]

            # Initialize search result fragments
            cur.execute('''SELECT room_num, old_num, description FROM ''' + sFacility + '''Room WHERE id = ?''', (roomid,))
            rooms = cur.fetchone()
            location = rooms[0]
            location_old = rooms[1]
            location_descr = rooms[2]
            bar = ' | '

            # Generate search result string, which must include all fragments matched by search operation
            search_result = ''

            if source:
                search_result += ' ' + source + bar

            if voltage:
                search_result += ' ' + voltage + 'V' + bar

            search_result = append_location( search_result, location, location_old, location_descr, bar )

            if objectType == 'Panel':
                # It's a panel; leave description empty and remove trailing bar delimiter
                description = ''
                if search_result:
                    search_result = search_result[:-3]
            else:
                # Not a panel; use description field from CSV file
                description = line[4].strip()
                search_result += ' "' + description + '"'

            if search_result.strip():
                search_result = name + ':' + search_result
            else:
                search_result = name

            cur.execute('''INSERT OR IGNORE INTO ''' + sFacility + '''CircuitObject (path, room_id, zone, voltage_id, object_type, description, tail, search_result, source )
                VALUES (?,?,?,?,?,?,?,?,?)''', (path, roomid, zone, volt_id, objectType, description, name, search_result, source))

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

    return tree_map, tree_map_root_path



def make_device_table( sFacility, tree_map ):

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

    return tree_map


def make_facility( sEnterprise, sFacility ):

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
        search_result TEXT,
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

    make_room_table( sFacility )

    tree_map, tree_map_root_path = make_circuit_object_table( sFacility )

    tree_map = make_device_table( sFacility, tree_map )


    # Save tree map in JSON format
    with open( 'E:\\xampp/htdocs/www/oops/database/' + sEnterprise + '/' + sFacility + '/tree.json', 'w' ) as outfile:
        json.dump( tree_map[tree_map_root_path], outfile )




def make_database( enterprise_object, facility_map ):

    #Builds SQLite database
    cur.executescript('''

        CREATE TABLE IF NOT EXISTS Enterprise (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            enterprise_name TEXT UNIQUE,
            enterprise_fullname TEXT UNIQUE
        );

        CREATE TABLE IF NOT EXISTS Facility (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            facility_name TEXT UNIQUE,
            facility_fullname TEXT UNIQUE
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

    # Initialize Enterprise table
    cur.execute( 'INSERT OR IGNORE INTO Enterprise (enterprise_name, enterprise_fullname) VALUES (?,?)', (enterprise_object["enterprise_name"], enterprise_object["enterprise_fullname"]) )
    conn.commit()

    # Initialize Facilities table
    for facility_object in facility_map:
        print( 'facility_object: ', facility_object )
        facility_name = facility_object["facility_name"]
        facility_fullname = facility_object["facility_fullname"]
        print( 'facility: <' + facility_name + '> <' + facility_fullname + '>' )
        cur.execute( 'INSERT OR IGNORE INTO Facility (facility_name, facility_fullname) VALUES (?,?)', (facility_name, facility_fullname) )

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

    if enterprise_object["enterprise_name"] == 'demo':
        dbCommon.add_interactive_user( cur, conn, 'system', 'demo', 'demo', 'Visitor', False, True, 'Big', 'Bird', 'nest@sesame.com', 'Sesame Street', 'Demo User', facility_id_csv )
    else:
        dbCommon.add_interactive_user( cur, conn, 'system', 'admin', 'admin', 'Administrator', False, True, 'Oscar', 'Grouch', 'trash@sesame.com', 'Sesame Street', 'Administrator', '' )
        dbCommon.add_interactive_user( cur, conn, 'system', 'tech', 'tech', 'Technician', False, True, 'Cookie', 'Monster', 'oatmeal@sesame.com', 'Sesame Street', 'Default Technician', facility_id_csv )
        dbCommon.add_interactive_user( cur, conn, 'system', 'test', 'test', 'Visitor', False, True, 'Kermit', 'Frog', 'green@sesame.com', 'Sesame Street', 'Default Visitor', facility_id_csv )

    cur.execute('''INSERT INTO Activity ( timestamp, username, event_type, target_table, target_column, target_value, description )
        VALUES (?,?,?,?,?,?,? )''', ( time.time(), 'system', dbCommon.dcEventTypes['database'], '', '', '', 'Start generating tables from CSV files' ) )

    conn.commit()


    for facility_object in facility_map:
        make_facility( enterprise_object["enterprise_name"], facility_object["facility_name"] )


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
    parser.add_argument('-n', dest='names', help='CSV file listing names of enterprise and its facilities' )
    args = parser.parse_args()

    iLine = 0
    facility_map = []
    enterprise_object = {}
    with open( args.names,'r') as f:
        lines = csv.reader(f)
        for line in lines:
            if iLine == 1:
                enterprise_name = line[0].strip()
                enterprise_fullname = line[1].strip()
                enterprise_object = { 'enterprise_name': enterprise_name, 'enterprise_fullname': enterprise_fullname }
            elif iLine > 1:
                facility_name = line[0].strip()
                facility_fullname = line[1].strip()
                facility_object = { 'facility_name': facility_name, 'facility_fullname': facility_fullname }
                facility_map.append( facility_object )

            iLine += 1


    # Create new empty databse
    sDbPath = 'E:/xampp/htdocs/www/oops/database/' + enterprise_name + '/database.sqlite'

    if os.path.exists( sDbPath ):
        os.remove( sDbPath )

    conn = sqlite3.connect( sDbPath )
    cur = conn.cursor()

    # Load the database
    make_database( enterprise_object, facility_map )
