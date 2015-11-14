import argparse, logging, os, sqlite3, urllib, urllib2

xboxApiKey = '6b9f4356b93ddf78c4fb24f799da1f11b907bb21'
xboxApiBase = 'https://xboxapi.com/v2/'
xboxUserId = '2533274953123046' # For DishiestOcean55: 2533274953123046
headers = {'X-AUTH' : xboxApiKey}

dbName = 'gameClips.db'

basePath = '/data/' # relative to script location
clipsPath = '/clips/'
grabsPath = '/grabs/'

### Path where this script resides ###
def getScriptPath():
    return os.path.dirname(os.path.realpath(__file__))

def getData():
	# TODO decide what is going to be returned by below functions (stats on what was added, failed, etc)
	pass


def getClips():
	pass


def getGrabs():
	pass


### Wrapper to add a list of items to the db
def addListToDb(l):
	con = getDb()
	c = con.cursor()

	for listItem in l:
		addItemToDb(listItem, c)

	con.commit()
	con.close()


def addItemToDb(i, c):
	t = ''
	if clipColumns[0]['colName'] in i:
		t = clipTableName
	elif grabColumns[0]['colName'] in i:
		t = grabTableName

	cols = []
	vals = []

	for col in i:
		cols.append(col['colName'])
		vals.append(i[col['colName']])

	try:
		s = "INSERT OR IGNORE INTO {tn} ({c}) VALUES ({v})".format(tn=t, c=SEP.join(cols), v=SEP.join(vals))
		c.execute(s)

		# TODO what does it return if ignored? does the last key inserted help here?
		# TODO add logic here (after we know it was successful/unique) to download the video and store it
		# TODO have a function that iterates over the table for empty file paths (download failed) and retries
		# Can add index to filepaths? non null / empty?
		# TODO add fields to grab and clip tables for local media file path

	except sqlite3.IntegrityError:
	    print('ERROR: Something happened - Integrity Error - Statement: \n\t'.format(s))


### Check the schema of the database and open a new one if necessary
def checkDatabase():
	con = getDb()

	### Test/Add table for gameclip db
	if something: # TODO add a check here (perhaps a schema check function)
		pass

	else:
		c = con.cursor()
		c.execute('CREATE TABLE {tn}'.format(tn=clipTableName))
		for col in clipColumns:
			c.execute("ALTER TABLE {tn} ADD COLUMN '{nf}' {ft} {p}"\
				.format(tn=clipTableName, nf=col['colName'], ft=col['colType'], p=col['primary']))
		
	con.commit()
	con.close()
	

### Lil somethin somethin to standardize getting a connection to the db
def getDb():
	# This line gets or creates (as needed) an sqlite3 db
	return  sqllite3.connect(os.path.join(getScriptPath(), basePath, dbName))


### Schema ### 
# TODO Move this to its own file
TEXT = 'TEXT'
INTEGER = 'INTEGER'
SEP = ', '

clipTableName = 'clips'
clipColumns = [{'colName' : 'gameClipId', # First position will be primary key
				'colType' : TEXT,
				'primary' : 'PRIMARY KEY'
			},{
				'colName' : "state",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "datePublished",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "dateRecorded",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "lastModified",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "userCaption",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "type",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "durationInSeconds",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "scid",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "titleId",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "rating",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "ratingCount",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "views",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "titleData",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "systemProperties",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "savedByUser",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "achievementId",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "greatestMomentId",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "thumbnails",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "gameClipUris",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "xuid",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "clipName",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "titleName",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "gameClipLocale",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "clipContentAttributes",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "deviceType",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "commentCount",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "likeCount",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "shareCount",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "partialViews",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "gameClipDetails",
				'colType' : TEXT,
				'primary' : ''
			}]



grabTableName = 'grabs'
grabColumns = [{
				'colName' : "screenshotId",
				'colType' : TEXT,
				'primary' : 'PRIMARY KEY'
			},{
				'colName' : "resolutionHeight",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "resolutionWidth",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "state",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "datePublished",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "dateTaken",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "lastModified",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "userCaption",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "gameClipDetails",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "type",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "scid",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "titleId",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "rating",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "ratingCount",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "views",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "titleData",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "systemProperties",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "savedByUser",
				'colType' : INTEGER,
				'primary' : ''
			},{
				'colName' : "achievementId",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "greatestMomentId",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "thumbnails",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "screenshotUris",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "xuid",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "screenshotName",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "screenshotLocale",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "screenshotContentAttributes",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "deviceType",
				'colType' : TEXT,
				'primary' : ''
			},{
				'colName' : "screenshotDetails",
				'colType' : TEXT,
				'primary' : ''
			}]


### Respond to call from command line ###
if __name__ == "__main__":
    global cwd
    cwd = os.getcwd()
    
    ### Arg Parsing ###
    
    # TODO decide on and adjust to match args and parsing

    parser = argparse.ArgumentParser()
    # parser.add_argument('name', help='Name of the project (and folder) to create', nargs='?', default='_stop_')
    # parser.add_argument('-c', '--contributors', dest='contributors', help='Contributors to the project', nargs=3, action='append', metavar=('cName', 'cEmail', 'cRank'))
    # parser.add_argument('-e', '--example', dest='example', help='Generate example folder', action='store_true')
    # parser.add_argument('-i', '--info', dest='info', help='Very short description of the project')
    # parser.add_argument('-s', '--scm', dest='scm', help='Which source control management you would like initialized', choices=['git', 'None'])
    # parser.add_argument('-t', '--template', dest='template', help="Template name (also used as the name of the template's enclosing folder)", default='Generic')

    parser.add_argument('-v', '--verbose', dest='verbosity', help='Increase verbosity (off/on/firehose)', action='count', default=0)
    parser.add_argument('dirs', help='Directories to check for duplicates', nargs='+')
    args = parser.parse_args()
    
    ### Initialize Logging ###
    if args.verbosity == 0:
        l = logging.WARNING
    elif args.verbosity == 1:
        l = logging.INFO
    else:
        l = logging.DEBUG

#   TODO remove, only for debuggin purposes
    l = logging.DEBUG
        
    logging.basicConfig(level=l, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.debug(str(args))

    dirs = massageInputDirs(args.dirs)

    checkForDupes(dirs)

    ### Reset working directory to original ###
    os.chdir(cwd)