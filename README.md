ais_satellite
=============

#Satellite AIS Data Parsing
this script will take as input a set of exact earth AIS csv files and will:
1. cleans the data following some basic criteria and outputs both clean data files and dirty data files
2. joins (inner) that data with an international database of ship names / MMSI / IMO / types / etc, to drop off some of wonky data 
3. converts these cleaned csv's to point shapefiles
4. converts the points data to lines shapefiles
**5. not yet implemented -- open source version of simple line density to show the 'lanes'.

 



