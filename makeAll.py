import os
import argparse

# Main program
if __name__ == '__main__':

    # Generate device table input file
    status = os.system( 'C:\\Users\Ayee\Anaconda3\python.exe "E:/ayee/Python/makeDb/makeDeviceTable.py" -i "E:/ayee/Python/makeDb/pathways.csv" -c "E:/ayee/Python/makeDb/devices.csv" -s "E:/Share/EA/AHS/devices.csv" -x "E:/Share/EA/AHS/devices.xlsx"' )

    if status == 0:
        # Generate database

        parser = argparse.ArgumentParser( description='script to extract room numbers from description column' )
        parser.add_argument( '-e', dest='enterprise', help='enterprise' )
        parser.add_argument( '-f', dest='facilities', help='comma-separated list of facility names' )
        args = parser.parse_args()

        status = os.system( 'C:\\Users\Ayee\Anaconda3\python.exe "E:/ayee/Python/makeDb/CreateDB.py" -e ' + args.enterprise + ' -f ' + args.facilities )

        if status != 0:
            print( 'failed to generate database. status=' + str( status ) )

    else:
        print( 'failed to generate device table. status=' + str(status) )
