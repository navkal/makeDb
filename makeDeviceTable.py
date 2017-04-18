import pandas as pd
import argparse
import re

# Regular expressions for extracting room numbers from text
patternOld = re.compile( r'\d{4}[a-zA-Z]?' )
patternNew = re.compile( r'(\d{3}(?:[a-fA-F]|\-\d{2}|(\d))?)' )
delimiter = '!#!#!'

# Make device table from input DataFrame
def makeDeviceTable( df ):

  # Remove irrelevant rows and columns
  df = df.dropna( subset=['devices'] )
  df = df[ [ 'path', 'devices' ] ]

  # Initialize device table
  deviceTable = pd.DataFrame( columns=['DeviceObj', 'Circuit', 'Location', 'Debug'#, 'Drop By Match', 'Drop By Search', 'Drop Different'
                                       ] )

  # Iterate over input DataFrame to build device table
  for row in df.itertuples():

    # Get list of devices from devices column
    devices = row.devices.split( '|' )

    # Iterate over devices
    for device in devices:

      # Initialize cells of next output row
      dev = ''
      old = ''
      new = ''
      unf = ''
      dbg = ''

      # Extract information from current device
      position = device.find( delimiter )
      if ( position == -1 ):
        # Delimiter not found
        dev = device
      else:
        # Delimiter found

        # Separate into device and location
        dev = device[:position]
        unf = device[position + len(delimiter):]

        # Look for special formats in location
        match = None
        matchOld = re.search( patternOld, unf )
        matchNew = re.search( patternNew, unf )
        if matchOld:
          match = matchOld
          old = match.group()
        elif matchNew:
          match = matchNew
          new = match.group()

        if match:
          # Preserve leading and trailing text dropped from match
          span = match.span()

      # Strip whitespace
      dev = dev.strip()
      unf = unf.strip()

      # Set location new or old room number, or unformatted text
      loc = new or old or unf

      # Preserve unformatted location if it differs from location
      if unf != loc:
        dbg = unf

      print( 's<' + device + '>' )
      print( 'd<' + dev + '>' )
      print( 'l<' + loc + '>' )
      print( 'd<' + dbg + '>' )
      print( "=======================================================" )

      # dropMatch = '' if isDeviceByMatch( dev ) else dev
      # dropSearch = '' if isDeviceBySearch( dev ) else dev
      # dropDiff = '' if dropMatch == dropSearch else 'DIFFERENT'
      if isDeviceByMatch( dev ):
        deviceTable.loc[ len( deviceTable ) ] = [ dev, row.path, loc, dbg#, dropMatch, dropSearch, dropDiff
                                                ]

  return deviceTable



patternNotDev = re.compile( r'blank|spare|panel|transformer|main', re.I )
def isDeviceByMatch( dev ):
  isDev = not re.match( patternNotDev, dev )
  return isDev

# def isDeviceBySearch( dev ):
#   isDev = not re.search( patternNotDev, dev )
#   return isDev


# Main program
if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='script to extract room numbers from description column')
  parser.add_argument('-i', dest='input_file',  help='name of input file')
  parser.add_argument('-c', dest='output_csv', help='name of output csv file')
  parser.add_argument('-d', dest='output_csv_for_db', help='name of output csv file for DB generator')
  parser.add_argument('-s', dest='output_csv_for_share', help='name of output csv file for share')
  parser.add_argument('-x', dest='output_xls', help='name of output xls file')
  args = parser.parse_args()

  df = pd.read_csv( args.input_file )
  deviceTable = makeDeviceTable( df )
  print( "=======================================================" )
  print( deviceTable.head() )


  # For testing: dump results to files
  if ( args.output_csv != None ):
    deviceTable.to_csv( args.output_csv, index=False )
  if ( args.output_csv_for_db != None ):
    deviceTable.to_csv( args.output_csv_for_db, index=False )
  if ( args.output_csv_for_share != None ):
    deviceTable.to_csv( args.output_csv_for_share, index=False )
  if ( args.output_xls != None ):
    deviceTable.to_excel( args.output_xls, index=False )
