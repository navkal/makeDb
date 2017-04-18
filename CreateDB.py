import sqlite3
import csv

conn = sqlite3.connect('AHSMap.sqlite')
cur = conn.cursor()

def get_room_index(room_number):
    #print( 'get_room_index for ', room_number )
    cur.execute('SELECT id FROM Room WHERE ? IN (room_num, old_num)', (room_number,))
    index = cur.fetchone()
    return index[0]

def get_circuit_index(circuit_path):
    cur.execute('SELECT id FROM CircuitObject WHERE path = ?', (circuit_path,))
    index = cur.fetchone()
    return index[0]

def get_voltage_index(voltage):
    print(voltage)
    cur.execute('SELECT id FROM Voltage WHERE ? IN (description)', (voltage,))
    index = cur.fetchone()
    return index[0]


#Builds SQLite database
cur.executescript('''

    DROP TABLE IF EXISTS Room;
    DROP TABLE IF EXISTS CircuitObject;
    DROP TABLE IF EXISTS Device;
    DROP TABLE IF EXISTS Voltage;


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
    parent TEXT
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




# bulids Room table 
with open('rooms.csv','r') as f:
    readfile = csv.reader(f)
    for line in readfile:
        print(line)
        if (line[0] == 'old'):
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

        parent = path.rsplit('.',maxsplit=1)[0]
        print(parent)
        if parent == path:
            parent = ''

        #print('inserting', path, roomid, zone, volt_id, objectType, desc)
        cur.execute('''INSERT OR IGNORE INTO CircuitObject (path, room_id, zone, voltage_id, object_type, description, parent)
            VALUES (?,?,?,?,?,?,?)''', (path, roomid, zone, volt_id, objectType, desc, parent))

        conn.commit()



with open ('devices.csv', 'r') as file:
    devicereader = csv.reader(file)

    for line in devicereader:
        print(line)

        if line[0] == 'DeviceObj':
            continue
        #if room is unknown, insert unknown as room



        #Handles Exceptions that should be fixed by fixing erroneous datapoints
        if line[2] == '':
            roomid = 'UNKNOWN'
        else:
            print('looking for ', line[2])
            try:
                roomid = get_room_index(line[2])
            except:
                print('THROWING RADICAL EXCEPTION FOR ' + line[2])
                cur.execute('''INSERT OR IGNORE INTO Room (room_num, old_num, location_type, description)
                            VALUES (?,?,?,? )''', (line[2], line[2], line[2], line[2]))
                conn.commit()
                roomid = get_room_index(line[2])



        panelid = get_circuit_index(line[1])
        description = line[0]
        parent = line[1].rsplit('.', maxsplit=1)[0]

        print(roomid, panelid, description, parent)
        cur.execute('''INSERT OR IGNORE INTO Device (room_id, panel_id, description, parent)
             VALUES (?,?,?,?)''', (roomid, panelid, description, parent))

        conn.commit()

# with open('pathways.csv','r') as file:
#     pathwayreader = csv.reader(file)
#
#     for line in pathwayreader:
#         if line[0] == '' or 'path':
#             continue
#
#         path = line[]
#         type = line[]
#         voltage = line[]
#         roomid = line[]
#
#         print(path, type, voltage, room)