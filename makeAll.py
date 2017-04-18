import os

# Generate device table input file
status = os.system( 'C:\\Users\Ayee\Anaconda3\python.exe "E:/ayee/Python/makeDb/makeDeviceTable.py" -i "E:/ayee/Python/makeDb/pathways.csv" -c "E:/ayee/Python/makeDb/devices.csv" -s "E:/Share/EA/AHS/devices.csv" -x "E:/Share/EA/AHS/devices.xlsx"' )

# Generate database
if status == 0:
  import CreateDB
else:
  print( 'failed to generate device table. status=' + str(status) )
