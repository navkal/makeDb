import sys
sys.path.insert(0, 'E:\\xampp/htdocs/www/oops/database')

import sqlite3
import csv
import time
import json
import argparse
import dbCommon
import os
import shutil
import natsort

conn = None
cur = None

missing_rooms = []

def get_room_id( room_number, sFacility='', refr='', refr_type='' ):
    cur.execute('SELECT id FROM '+sFacility+'_Room WHERE ? IN (room_num, old_num)', (room_number,))

    try:
        # Try to get room from database
        room_id = cur.fetchone()[0]
    except:
        # Track missing room
        global missing_rooms
        missing_rooms.append( { 'loc': room_number, 'refr': refr, 'refr_type': refr_type } )

        # Work around missing room by adding it to the database
        cur.execute('''INSERT OR IGNORE INTO '''+sFacility+'''_Room (room_num, old_num, location_type, description)
                    VALUES (?,?,?,? )''', (room_number, room_number, '', room_number))
        conn.commit()

        # Retry
        room_id = get_room_id( room_number, sFacility )

    return room_id


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

            cur.execute('''INSERT OR IGNORE INTO ''' + sFacility + '''_Room (room_num, old_num, location_type, description)
                VALUES (?,?,?,? )''', (new_num, old_num, '', description) )

            conn.commit()


def make_image_cache( sEnterprise, sFacility ):

    # Create new empty target directory
    sTargetDir = 'E:/xampp/htdocs/www/oops/database/' + sEnterprise + '/' + sFacility + '/images/';
    if os.path.exists( sTargetDir ):
        shutil.rmtree( sTargetDir )
    os.makedirs( sTargetDir )

    # Get list of image files in source directory
    sSourceDir = './images/' + sEnterprise + '/' + sFacility + '/'
    aFiles = os.listdir( sSourceDir )

    # Traverse list of image files, copying each one to target
    for sSourceFilename in aFiles:
        sPath = '.'.join( sSourceFilename.split('.')[0:-1] )
        sId = dbCommon.path_to_id( cur, sPath, sFacility )
        sTargetFilename = sId + '.jpg'
        print( sFacility + " images [copying '" + sSourceFilename + "' to '" + sTargetFilename + "'" )
        shutil.copyfile( sSourceDir + sSourceFilename, sTargetDir + sTargetFilename )


def make_distribution_table( sFacility ):

    tree_map = {}

    with open(sFacility + '_distribution.csv','r') as file:
        circuitreader = csv.reader(file)

        path_phase_map = {}

        for line in circuitreader:
            print( sFacility + ' distribution', line )
            path = line[0].strip()
            if path == 'path' or path == '':
                continue

            object_type = line[1].strip().title()
            cur.execute('''INSERT OR IGNORE INTO DistributionObjectType (object_type) VALUES (?)''', (object_type,))
            object_type_id = dbCommon.object_type_to_id( cur, object_type )

            three_phase = line[2]

            phaseB = line[3]
            phaseC = line[4]
            if phaseB or phaseC:
                path_phase_map[path] = { 'phaseB': phaseB, 'phaseC': phaseC }

            voltage = line[5].strip()
            voltage_id = ''
            if voltage:
                cur.execute('''INSERT OR IGNORE INTO Voltage (voltage) VALUES (?)''', (voltage,))
                voltage_id = dbCommon.voltage_to_id( cur, voltage )

            room_id = get_room_id( line[6].strip(), sFacility, path, object_type )

            # Initialize path and path fragments
            pathsplit = path.split('.')
            tail = pathsplit[-1]

            if len( pathsplit ) == 1:
              source = ''
            else:
              source = pathsplit[-2]

            if object_type == 'Panel':
                description = ''
            else:
                description = line[7].strip()

            cur.execute('''INSERT OR IGNORE INTO ''' + sFacility + '''_Distribution
                ( path, object_type_id, three_phase, parent_id, phase_b_parent_id, phase_c_parent_id, voltage_id, room_id, description, tail, search_result, source )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''', ( path, object_type_id, three_phase, '', '', '', voltage_id, room_id, description, tail, '', source ) )

            # Add node to tree map
            tree_map[path] = { 'name': path.rsplit( '.' )[-1], 'children': [] }

            conn.commit()


    # Load parent ID in table and parent-child relationship in tree map
    cur.execute( 'SELECT id, path FROM ' + sFacility + '_Distribution' )
    rows = cur.fetchall()

    tree_map_root_path = ''

    for row in rows:
        row_id = row[0]
        row_path = row[1]
        parent_path = row_path.rsplit( '.', maxsplit=1 )[0]

        phase_b_parent_id = ''
        phase_c_parent_id = ''

        if row_path in path_phase_map:

            parent_parent_path = parent_path.rsplit( '.', maxsplit=1 )[0]

            if 'phaseB' in path_phase_map[row_path]:
                phase_b_parent_path = parent_parent_path + '.' + path_phase_map[row_path]['phaseB']
                cur.execute( 'SELECT id FROM ' + sFacility + '_Distribution WHERE path = ?', (phase_b_parent_path,) )
                phase_b_parent_id = cur.fetchone()[0]

            if 'phaseC' in path_phase_map[row_path]:
                phase_c_parent_path = parent_parent_path + '.' + path_phase_map[row_path]['phaseC']
                cur.execute( 'SELECT id FROM ' + sFacility + '_Distribution WHERE path = ?', (phase_c_parent_path,) )
                phase_c_parent_id = cur.fetchone()[0]

            print( sFacility + ' phases [B=' + phase_b_parent_path + '  C=' + phase_c_parent_path + ']' )

        if parent_path == row_path:
            # Save root path
            tree_map_root_path = row_path
        else:
            # Link current node to its parent in tree map
            tree_map[parent_path]['children'] += [ tree_map[row_path] ]

            cur.execute( 'SELECT id FROM ' + sFacility + '_Distribution WHERE path = ?', (parent_path,) )
            parent_row = cur.fetchone()
            parent_id = parent_row[0]
            cur.execute( 'UPDATE ' + sFacility + '_Distribution SET parent_id=?, phase_b_parent_id=?, phase_c_parent_id=? WHERE id=?', ( parent_id, phase_b_parent_id, phase_c_parent_id, row_id ) )

    conn.commit()

    # Propagate three-phase property from panels to circuits
    cur.execute( 'SELECT ' + sFacility + '_Distribution.id, three_phase FROM ' + sFacility + '_Distribution LEFT JOIN DistributionObjectType ON object_type_id=DistributionObjectType.id WHERE DistributionObjectType.object_type="Panel"' )
    panel_rows = cur.fetchall()
    for panel_row in panel_rows:
        panel_id = panel_row[0]
        three_phase = panel_row[1]
        cur.execute( 'UPDATE ' + sFacility + '_Distribution SET three_phase=? WHERE parent_id=?', ( three_phase, panel_id, ) )

    # Propagate voltage property from root to transformers and from transformers to leaves
    bDone = False

    while not bDone:
        cur.execute( 'SELECT COUNT(*) FROM ' + sFacility + '_Distribution WHERE voltage_id=""' )
        count = cur.fetchone()[0]

        if count == 0:
            # No more empty voltage_id cells
            bDone = True
        else:
            # Get all rows with non-empty voltage_id values
            cur.execute( 'SELECT id, voltage_id FROM ' + sFacility + '_Distribution WHERE voltage_id<>""' )
            voltage_rows = cur.fetchall()

            for voltage_row in voltage_rows:
                row_id = voltage_row[0]
                voltage_id = voltage_row[1]
                # Propagate voltage_id to children that don't already have a voltage_id set
                cur.execute( 'UPDATE ' + sFacility + '_Distribution SET voltage_id=? WHERE voltage_id="" AND parent_id=?', ( voltage_id, row_id, ) )


    conn.commit()


    # Generate search results for all Distribution table rows

    dist_table = sFacility + '_Distribution'
    room_table = sFacility + '_Room'

    cur.execute( '''
        SELECT
            ''' + dist_table + '''.id,
            ''' + dist_table + '''.source,
            Voltage.voltage,
            ''' + room_table + '''.room_num,
            ''' + room_table + '''.old_num,
            ''' + room_table + '''.description,
            ''' + dist_table + '''.description,
            ''' + dist_table + '''.tail
        FROM ''' + dist_table + '''
            LEFT JOIN Voltage ON ''' + dist_table + '''.voltage_id=Voltage.id
            LEFT JOIN ''' + room_table + ''' ON ''' + dist_table + '''.room_id=''' + room_table + '''.id
        ''')

    rows = cur.fetchall()

    for row in rows:

        id = row[0]
        source = row[1]
        voltage = row[2]
        location = row[3]
        location_old = row[4]
        location_descr = row[5]
        description = row[6]
        tail = row[7]

        search_result = dbCommon.make_search_result( source, voltage, location, location_old, location_descr, description, tail );

        print( sFacility + ' search_result [' + search_result + ']')

        cur.execute( 'UPDATE ' + sFacility + '_Distribution SET search_result=? WHERE id=?', ( search_result, id ) )

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

            circuit_path = line[1].strip()
            parentid = dbCommon.path_to_id( cur, circuit_path, sFacility )

            loc = line[2].strip()
            if loc == '':
                roomid = ''
                location = ''
                location_old = ''
                location_descr = ''
            else:
                roomid = get_room_id( loc, sFacility, "'" + name + "' on Circuit '" + circuit_path + "'", 'Device' )
                cur.execute('''SELECT room_num, old_num, description FROM ''' + sFacility + '''_Room WHERE id = ?''', (roomid,))
                rooms = cur.fetchone()
                location = rooms[0]
                location_old = rooms[1]
                location_descr = rooms[2]

            # Generate description
            desc = dbCommon.format_device_description( name, location, location_old, location_descr )

            cur.execute('''INSERT OR IGNORE INTO ''' + sFacility + '''_Device (room_id, parent_id, description, name)
                 VALUES (?,?,?,?)''', (roomid, parentid, desc, name))

            conn.commit()


    # Link devices into tree map
    cur.execute( 'SELECT id, parent_id, name FROM ' + sFacility + '_Device' )
    rows = cur.fetchall()

    for row in rows:
        row_id = row[0]
        parent_id = row[1]
        row_name = row[2]

        cur.execute( 'SELECT path FROM ' + sFacility + '_Distribution WHERE id = ?', (parent_id,) )
        parent_path = cur.fetchone()[0]
        row_path = parent_path + '.' + str( row_id )

        # Insert node in tree map and link to parent
        tree_map[row_path] = { 'name': row_name, 'children': [] }
        tree_map[parent_path]['children'] += [ tree_map[row_path] ]

    return tree_map


def make_facility( sEnterprise, sFacility ):

    roomFields = '''
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        room_num TEXT,
        old_num TEXT,
        location_type TEXT,
        description TEXT
    '''

    distributionFields = '''
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        path TEXT UNIQUE,
        object_type_id TEXT,
        three_phase INTEGER,
        parent_id INTEGER,
        phase_b_parent_id INTEGER,
        phase_c_parent_id INTEGER,
        voltage_id INTEGER,
        room_id INTEGER,
        description TEXT,
        tail TEXT,
        search_result TEXT,
        source TEXT
    '''

    deviceFields = '''
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        room_id INTEGER,
        parent_id INTEGER,
        description TEXT,
        power TEXT,
        name TEXT
    '''

    removeField = ', remove_id INTEGER'

    cur.executescript('''
        CREATE TABLE IF NOT EXISTS ''' + sFacility + '''_Room (''' + roomFields + ''');
        CREATE TABLE IF NOT EXISTS ''' + sFacility + '''_Removed_Room (''' + roomFields + removeField + ''');

        CREATE TABLE IF NOT EXISTS ''' + sFacility + '''_Distribution (''' + distributionFields + ''');
        CREATE TABLE IF NOT EXISTS ''' + sFacility + '''_Removed_Distribution (''' + distributionFields + removeField + ''');

        CREATE TABLE IF NOT EXISTS ''' + sFacility + '''_Device (''' + deviceFields + ''' );
        CREATE TABLE IF NOT EXISTS ''' + sFacility + '''_Removed_Device (''' + deviceFields + removeField + ''' );

        CREATE TABLE IF NOT EXISTS ''' + sFacility + '''_Recycle (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            remove_timestamp FLOAT,
            remove_object_type TEXT,
            parent_path TEXT,
            loc_new TEXT,
            loc_old TEXT,
            loc_descr TEXT,
            remove_comment TEXT,
            remove_object_id INTEGER
        );
    ''')

    make_room_table( sFacility )

    ( tree_map, tree_map_root_path ) = make_distribution_table( sFacility )

    tree_map = make_device_table( sFacility, tree_map )


    # Save tree map in JSON format
    with open( 'E:\\xampp/htdocs/www/oops/database/' + sEnterprise + '/' + sFacility + '/tree.json', 'w' ) as outfile:
        json.dump( tree_map[tree_map_root_path], outfile )

    make_image_cache( sEnterprise, sFacility )




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
            event_type TEXT,
            username TEXT,
            facility_id INTEGER,
            event_target TEXT,
            event_result TEXT,
            target_object_type TEXT,
            target_object_id INTEGER
        );

        CREATE TABLE IF NOT EXISTS Voltage (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        voltage TEXT UNIQUE
        );

        CREATE TABLE IF NOT EXISTS DistributionObjectType (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        object_type TEXT UNIQUE
        );

    ''')

    # Initialize Voltage table
    cur.execute( '''INSERT OR IGNORE INTO Voltage ( voltage ) VALUES (?)''', ('277/480',) )
    cur.execute( '''INSERT OR IGNORE INTO Voltage ( voltage ) VALUES (?)''', ('120/208',) )

    # Initialize DistributionObjectType table
    cur.execute( '''INSERT OR IGNORE INTO DistributionObjectType ( object_type ) VALUES (?)''', ('Panel',) )
    cur.execute( '''INSERT OR IGNORE INTO DistributionObjectType ( object_type ) VALUES (?)''', ('Transformer',) )
    cur.execute( '''INSERT OR IGNORE INTO DistributionObjectType ( object_type ) VALUES (?)''', ('Circuit',) )

    # Initialize Role table
    cur.execute( '''INSERT OR IGNORE INTO Role ( role ) VALUES (?)''', ('Administrator',) )
    cur.execute( '''INSERT OR IGNORE INTO Role ( role ) VALUES (?)''', ('Technician',) )
    cur.execute( '''INSERT OR IGNORE INTO Role ( role ) VALUES (?)''', ('Visitor',) )

    # Initialize Enterprise table
    cur.execute( 'INSERT OR IGNORE INTO Enterprise (enterprise_name, enterprise_fullname) VALUES (?,?)', (enterprise_object["enterprise_name"], enterprise_object["enterprise_fullname"]) )

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

    cur.execute('''INSERT INTO Activity ( timestamp, event_type, username, facility_id, event_target, event_result, target_object_type, target_object_id )
        VALUES (?,?,?,?,?,?,?,?)''', ( time.time(), dbCommon.dcEventTypes['database'], 'system', '', '', 'Started generating tables from CSV files', '', ''  ) )

    conn.commit()


    for facility_object in facility_map:

        # Initialize missing rooms list
        sFacility = facility_object["facility_name"]
        global missing_rooms
        missing_rooms = []

        # Optionally remove missing rooms file
        sMissingRoomsFilename = sFacility + '_missing_rooms.csv'
        if os.path.exists( sMissingRoomsFilename ):
            os.remove( sMissingRoomsFilename )

        # Make facility in database
        make_facility( enterprise_object["enterprise_name"], sFacility )

        # Dump referenced rooms that are missing from rooms.csv
        if len( missing_rooms ) > 0:
            missing_rooms = natsort.natsorted( missing_rooms, key=lambda x: x['refr'] )

            with open( sMissingRoomsFilename, 'w' ) as missing_rooms_file:
                writer = csv.writer( missing_rooms_file, lineterminator='\n' )
                writer.writerow( [ 'Referrer', 'Referrer Type', 'Location' ] )

                for missing_room in missing_rooms:
                    writer.writerow( [ missing_room['refr'], missing_room['refr_type'], missing_room['loc'] ] )

            print( '' )
            print( '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' )
            print( '   ' + str( len( missing_rooms ) ) + ' rooms MISSING from ' + sFacility + '_rooms.csv.' )
            print( '   See ' + sMissingRoomsFilename + ' for details.' )
            print( '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' )
            print( '' )


        # Find all rooms that are not referenced by any Distribution or Device object

        # Optionally remove unused rooms file
        sUnusedRoomsFilename = sFacility + '_unused_rooms.csv'
        if os.path.exists( sUnusedRoomsFilename ):
            os.remove( sUnusedRoomsFilename )

        unused_rooms = []
        cur.execute( 'SELECT id, room_num, old_num, description FROM ' + sFacility + '_Room' )
        room_rows = cur.fetchall()

        for room_row in room_rows:

            # Get current room ID
            room_id = room_row[0]

            # Determine whether any Distribution objects refer to current room
            cur.execute( 'SELECT COUNT(*) FROM ' + sFacility + '_Distribution WHERE room_id=?', ( room_id, ) )
            count = cur.fetchone()[0]

            if count == 0:
                # Determine whether any Distribution objects refer to current room
                cur.execute( 'SELECT COUNT(*) FROM ' + sFacility + '_Device WHERE room_id=?', ( room_id, ) )
                count = cur.fetchone()[0]

            if count == 0:
                unused_rooms.append( { 'loc': room_row[1], 'loc_old': room_row[2], 'loc_descr': room_row[3] } )

        if len( unused_rooms ) > 0:

            with open( sUnusedRoomsFilename, 'w' ) as unused_rooms_file:
                writer = csv.writer( unused_rooms_file, lineterminator='\n' )
                writer.writerow( [ 'Location', 'Old Location', 'Location Description' ] )

                for unused_room in unused_rooms:
                    writer.writerow( [ unused_room['loc'], unused_room['loc_old'], unused_room['loc_descr'] ] )

            print( '' )
            print( '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' )
            print( '   ' + str( len( unused_rooms ) ) + ' UNUSED rooms in ' + sFacility + '_rooms.csv.' )
            print( '   See ' + sUnusedRoomsFilename + ' for details.' )
            print( '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' )
            print( '' )



    cur.execute('''INSERT INTO Activity ( timestamp, event_type, username, facility_id, event_target, event_result, target_object_type, target_object_id )
        VALUES (?,?,?,?,?,?,?,?)''', ( time.time(), dbCommon.dcEventTypes['database'], 'system', '', '', 'Finished generating tables from CSV files', '', ''  ) )

    conn.commit()



################
# Main program #
################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='script to generate database')
    parser.add_argument( '-n', dest='names', help='CSV file listing names of enterprise and its facilities' )
    parser.add_argument( '-v', dest='device_table', action='store_true', help='flag to make device table' )
    parser.add_argument( '-b', dest='database', action='store_true', help='flag to make database' )
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


    # Optionally make device tables for all facilities in the enterprise
    if args.device_table:

        printline = "-----=====-----=====-----=====-----\n"

        for facility_object in facility_map:

            # Format command to generate device table for current facility
            facility_name = facility_object["facility_name"]
            filename_prefix = facility_name + '_'
            input_filename = filename_prefix + 'distribution.csv'
            output_filename = filename_prefix + 'devices.csv'
            command = 'C:\\Users\Ayee\Anaconda3\python.exe makeDeviceTable.py -i ' + input_filename + ' -c ' + output_filename

            # Echo command
            print( printline + printline + "Making device table for facility '" + enterprise_name + '/' + facility_name + "'" )
            print( printline + 'Command:' )
            print( command )
            print( printline + printline )

            # Execute command
            status = os.system( command )

            if status != 0:
                print( 'Failed to make device table from ' + input_filename )
                exit(1)


    # Optionally make database for the enterprise
    if args.database:
        print( '---' )
        print( "Making database for enterprise '" + enterprise_name + "'" )
        print( '---' )

        # Create new empty databse
        sDbPath = 'E:/xampp/htdocs/www/oops/database/' + enterprise_name + '/database.sqlite'

        if os.path.exists( sDbPath ):
            os.remove( sDbPath )

        conn = sqlite3.connect( sDbPath )
        cur = conn.cursor()

        # Load the database
        make_database( enterprise_object, facility_map )
