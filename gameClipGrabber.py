import argparse, errno, json, logging, os, pprint, sqlite3, sys, urllib, urllib2

xboxApiKey = '6b9f4356b93ddf78c4fb24f799da1f11b907bb21'
xboxApiBase = 'https://xboxapi.com/v2/'
xboxUserId = '2533274953123046' # For DishiestOcean55: 2533274953123046, for BearBiever: 2533274943564661
headers = {'X-AUTH' : xboxApiKey}
# These urls are for all the game items, it might be possible/desireable to only get the saved ones
clipsUrl = 'game-clips'
grabsUrl = 'screenshots'
getGamerTagUrl = 'gamertag/'
getXuidUrl = 'xuid/'

dbName = 'gameClips.db'

basePath = 'data/' # relative to script location
clipsPath = 'clips/'
grabsPath = 'grabs/'

### Path where this script resides ###
def getScriptPath():
    return os.path.dirname(os.path.realpath(__file__))


def getData(xboxId):
    logging.info('Getting all data')
    # TODO decide what is going to be returned by below functions (stats on what was added, failed, etc)

    checkForXboxId(xboxId)

    result = []
    result.append(getClips(xboxId))
    result.append(getGrabs(xboxId))
    return result


def getClips(xboxId):
    logging.info('Getting all the clips')
    url = xboxApiBase + xboxId + '/' + clipsUrl
    req = getReq(url)
    response = urllib2.urlopen(req)
    data = json.loads(response.read()) # TODO verify this

    result = addListToDb(data)

    return result # TODO change this to be more helpful (success true/false, error, etc)


def getGrabs(xboxId):
    logging.info('Getting all the grabs')

    url = xboxApiBase + xboxId + '/' + grabsUrl
    req = getReq(url)
    response = urllib2.urlopen(req)
    data = json.loads(response.read())

    result = addListToDb(data)

    return result # TODO change this to be more helpful (success true/false, error, etc)


def checkForXboxId(xuid):
    con = getDb()
    c = con.cursor()

    logging.info('Checking for local details for xuid: ' + xuid)

    s = "SELECT {gt} FROM {at} WHERE {xidn}={xid}"\
        .format(gt='gamertag', at=accountTable['name'], \
            xidn=accountTable['primaryCol']['colName'], xid=xuid)
    logging.debug('Statement is: \n\t' + s)
    c.execute(s)
    id_exists = c.fetchone()

    con.commit()
    con.close()

    if not id_exists:
        logging.debug('Account not found, attempting population')
        addAccountDetails(getInfosFromXuids([xuid]))


def addAccountDetails(infos):
    # TODO play with other endpoints (like profile or gamercard might be useful)
    con = getDb()
    c = con.cursor()
    for i in infos:
        logging.debug('Attempting add of account gamertag: ' + i['gamertag'])
        s = "INSERT OR IGNORE INTO {tn} ({idf}, {gt}, {gtc}) VALUES ({idv}, '{gtv}', '{gtcv}')".\
            format(tn=accountTable['name'], idf=accountTable['primaryCol']['colName'], gt='gamertag',\
                gtc='gamertagcompare', idv=i['xuid'], gtv=i['gamertag'], gtcv=i['gamertag'].lower())
        logging.debug('Statement is: \n\t' + s)
        c.execute(s)
    con.commit()
    con.close()


def getInfosFromXuids(xids):
    logging.info('Getting gamertags from xuids')
# TODO consider using named tuples for 'infos'
    gts = []
    for xid in xids:
        url = xboxApiBase + getGamerTagUrl + xid
        req = getReq(url)
        response = urllib2.urlopen(req)
        gts.append({
            'xuid' : xid,
            'gamertag' : response.read()
            })
            # TODO have some validation around this, maybe check http code

    return gts


def getInfosFromGamertags(gts):
    logging.info('Getting xuids from gamertags')

    xuids = []
    for g in gts:
        url = xboxApiBase + getXuidUrl + g
        req = getReq(url)
        response = urllib2.urlopen(req)
        xuids.append({
            'xuid' : response.read(),
            'gamertag' : g
            }) # TODO have some validation around this, maybe check http code

    return xuids


### Wrapper to add a list of items to the db
def addListToDb(l):
    logging.info('Adding list to database')
    con = getDb()
    c = con.cursor()

    res = []
    for listItem in l:
        res.append(addItemToDb(listItem, c))

    con.commit()
    con.close()

    return res


def addItemToDb(i, c):
    t = ''
    if clipTable['primaryCol']['colName'] in i:
        t = clipTable['name']
        n = clipTable['primaryCol']['colName'] + ' : ' + i[clipTable['primaryCol']['colName']]
    elif grabTable['primaryCol']['colName'] in i:
        t = grabTable['name']
        n = grabTable['primaryCol']['colName'] + ' : ' + i[grabTable['primaryCol']['colName']]


    logging.info('Processing item: ' + n)

    cols = []
    vals = []
    skipped = []

    # logging.debug(i)

    for col in i:
        if not str(i[col]):
            skipped.append({col, i[col]})
        else:
            cols.append(col)
            val = i[col]
            # logging.debug('Before: ' + str(val))
            if not is_number(val):
                tmp = str(val)
                tmp = tmp.replace("u'", '"')
                tmp = tmp.replace("'",'"')
                val = "'" + str(tmp) + "'"
            # logging.debug('After: ' + str(val))
            vals.append(str(val))

    try:
        s = "INSERT OR IGNORE INTO {tn} ({c}) VALUES ({v})".format(tn=t, c=SEP.join(cols), v=SEP.join(vals))
        # logging.debug('Statement is: \n\t' + s)
        c.execute(s)
        # TODO what does it return if ignored? does the last key inserted help here?
        # TODO add logic here (after we know it was successful/unique) to download the video and store it
    except sqlite3.IntegrityError:
        logging.warning('ERROR: Something happened - Integrity Error - Statement: \n\t'.format(s))

    return i # TODO fill this out


def downloadMissingData(inTables, maxNum=float("inf")):
    logging.info('Getting ready to download all missing data (clips/grabs)')

    counter = 1

    con = getDb()
    c = con.cursor()

    for t in inTables:
        if counter > maxNum:
            logging.info('Counter reached max: ' + str(counter))
            continue

        logging.info('Looking at database table: ' + t['name'])
        logging.debug('Grabbing candidates')
        selArr = [t['primaryCol']['colName'], 'titleName', 'deviceType', t['downloadCol'], 'datePublished', 'xuid']
        s = "SELECT {sel} FROM {tn} WHERE ({cn} = NULL) OR ({cn} IS NULL)"\
            .format(sel=SEP.join(selArr), tn=t['name'], cn='localDiskPath') # TODO this and below shouldnt be hardcoded really
        logging.debug('Statement is: \n\t' + s)
        c.execute(s)

        all_rows = c.fetchall()

        # TODO add decorator for tqdm here
        for r in all_rows:
            if counter > maxNum:
                continue

            logging.debug('Getting Account details')
            s = "SELECT {gt} FROM {at} WHERE {xidn}={xid}"\
                .format(gt='gamertag', at=accountTable['name'], \
                    xidn=accountTable['primaryCol']['colName'], xid=r[5])
            logging.debug('Statement is: \n\t' + s)
            c.execute(s)

            gt = c.fetchone()
            ### Top layer is gamer tag
            d = os.path.join(getScriptPath(), basePath, gt[0])
            mkDirDashP(d)

            ### Next layer is game
            d = os.path.join(d, r[1])
            mkDirDashP(d)

            ### Next layer is platform
            ### Commenting out since I currently dont care about multiple devices (xboxone, etc)
            # d = os.path.join(d, r[2])
            # mkDirDashP(d)

            cnt = 0
            uris = json.loads(r[3])
            downed = []
            totalSuccess = False
            for v in uris:
                if counter > maxNum:
                    logging.info('Counter reached max: ' + str(counter))
                    continue
                # TODO log this back to the DB (if success)
                dateString = r[4][:19].replace(':','')
                fn = dateString + '_' + r[0][:7] + '_' + str(cnt)
                # TODO parse uri for this instead of hardcode
                if t['name'] is 'clips':
                    fn = fn + '.mp4'
                elif t['name'] is 'grabs':
                    fn = fn + '.png'
                fn = os.path.join(d,fn)
                success = downloadFile(v['uri'], fn)

                cnt += 1
                if success:
                    totalSuccess = True
                    downed.append(fn)
                    counter += 1

            if totalSuccess:
                # Add back the paths to the db
                logging.debug('Adding file paths back to db')
                s = "UPDATE {tn} SET {c}=('{p}') WHERE {idf}=('{id}')"\
                    .format(tn=t['name'], c='localDiskPath', p=','.join(downed), \
                        idf=t['primaryCol']['colName'], id=r[0])
                logging.debug('Statement is: \n\t' + s)
                c.execute(s)

    con.commit()
    con.close()


### Initially http://stackoverflow.com/a/22776/286994
### Then http://blog.radevic.com/2012/07/python-download-url-to-file-with.html
def downloadFile(url, file_name):
    req = getReq(url)
    logging.info('Downloading: {0} \n\tUrl: {1}'.format(file_name, url))
    try:
        u = urllib2.urlopen(req)
        f = open(file_name, 'wb')
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])
        logging.debug('Download response info: \n\t' + str(u.info()))
        arr = file_name.split('/')
        shortfn = arr[-1]
        game = arr[-2]

        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = u.read(block_sz)
            if not buffer:
                break

            file_size_dl += len(buffer)
            f.write(buffer)
            status = r"%s/%s - %10d  [%3.2f%%]" % (game, shortfn, file_size_dl, file_size_dl * 100. / file_size)
            status = status + chr(8)*(len(status)+1)
            sys.stdout.write(status)

        f.close()
        sys.stdout.write('\n')
        logging.debug('Download Success')
        return True
    except urllib2.HTTPError:
        # TODO handle errors here
        logging.warning('Yo something happened')
        return False


def getReq(url):
    # TODO implement with headers from above
    req = urllib2.Request(url)
    req.add_header('X-AUTH', xboxApiKey)
    return req


### Check the schema of the database and open a new one if necessary
### http://sebastianraschka.com/Articles/2014_sqlite_in_python_tutorial.html (this method and below)
# def checkDatabase(inTables, inIndexes):
def checkDatabase(inTables):

    logging.info('Checking db consistency')

    # TODO check if database actually exists at all?

    con = getDb()
    c = con.cursor()

    missingTables = []

    for t in inTables:
        logging.debug('Looking at table: ' + t['name'])
        s = 'SELECT name FROM sqlite_master WHERE type = \'table\' AND name=\'{tn}\''\
            .format(tn=t['name'])
        logging.debug('Statement is: \n\t' + s)
        c.execute(s)
        table_exists = c.fetchone()
        if not table_exists:
            # Table is not in the db
            logging.info('Table was missing: ' + t['name'])
            missingTables.append(t)
            continue


        # Retrieve column information
        # Every column will be represented by a tuple with the following attributes:
        # (id, name, type, notnull, default_value, primary_key)
        logging.debug('Checking columns in table: ' + t['name'])
        s = 'PRAGMA TABLE_INFO({})'.format(t['name'])
        c.execute(s)
        tups = c.fetchall()

        # collect names in a list
        names = [tup[1] for tup in tups]
        # logging.debug('Column Names(?): \n\t' + str(names))

        missingCols = []
        for col in t['columns']:
            if col['colName'] in names:
                names.remove(col['colName'])
            else:
                missingCols.append(col)

        logging.debug('Missing columns: \n\t' + str(missingCols))
        # TODO FOR NOW if any columns are missing, add em back
        if len(missingCols) > 0:
            try:
                for col in missingCols:
                    # TODO this causes a problem if our intended primary key is missing, and there is already a primary key present
                    logging.debug('Checking for column: ' + col)
                    c.execute("ALTER TABLE {tn} ADD COLUMN '{nf}' {ft} {p}"\
                        .format(tn=t['name'], nf=col['colName'], ft=col['colType'], p=col['modify']))
            except:
                logging.debug('There was an exception adding a missing column, blowing it away')
                c.execute('DROP TABLE {}'.format(t['name']))
                # TODO add to missing table array

    # TODO also check indexes?
    con.commit()
    con.close()

    # TODO remove this hack with an actual check that doesn't blow away the db
    # src = os.path.join(getDbPath(), dbName)
    # dest = os.path.join(getDbPath(), dbName + '_1')
    # logging.debug('Backing up db from: \n\t' + src)
    # logging.debug('To: \n\t' + dest)
    # os.rename(src, dest)

    # createDatabase(missingTables, inIndexes)
    createDatabase(missingTables)


# TODO could fix some of the checking problems by passing in a list of tables and a list of indexes here (that way we could build lists of missing tables to pass in)
# def createDatabase(inTables, inIndexes):
def createDatabase(inTables):
    logging.info('Creating database with tables: ' + str([x['name'] for x in inTables]))
    # logging.info('Creating database with indexes: ' + str([x['name'] for x in inIndexes]))
    con = getDb()
    c = con.cursor()

    # TODO If excited, add verification here that tables and indexes are formatted properly

    ### Create the Tables
    for t in inTables:
        logging.info('Creating database table: ' + t['name'])
        nn = ''
        if 'notNullCols' in t:
            nn = SEP
            nn = nn + SEP.join([(x['colName'] + ' ' + x['colType'] + ' ' + x['modify']) for x in t['notNullCols']])

        s = 'CREATE TABLE {tn} ({pk} {ft}{notn})'\
            .format(tn=t['name'], pk=t['primaryCol']['colName'], \
                ft=t['primaryCol']['colType'] + ' ' + t['primaryCol']['modify'], notn=nn)
        logging.debug('Statement is: \n\t' + s)
        c.execute(s)

        ### Add all the columns and modifiers
        logging.info('Adding other columns now')
        for col in t['columns']:
            s = "ALTER TABLE {tn} ADD COLUMN '{nf}' {ft} {p}"\
                .format(tn=t['name'], nf=col['colName'], ft=col['colType'], p=col['modify'])
            logging.debug('Statement is: \n\t' + s)
            c.execute(s)

    ### Create the Indexes
    # logging.info('Adding indexes now')
    # for i in inIndexes:
    #     s = 'CREATE INDEX {idx} ON {tn}{cols} WHERE {w}'\
    #         .format(idx=i['name'], tn=i['table'], cols=i['columns'], w=i['where'])
    #     logging.debug('Statement is: \n\t' + s)
    #     c.execute(s) 

    con.commit()
    con.close()
    

### Lil somethin somethin to standardize getting a connection to the db
def getDb():
    # This line gets or creates (as needed) an sqlite3 db
    return sqlite3.connect(os.path.join(getDbPath(), dbName))


def getDbPath():
    mkDirDashP(os.path.join(getScriptPath(), basePath))
    return os.path.join(getScriptPath(), basePath)


### Attempts to make a directory, and if it raises an error, it checks to see if it is a "Folder exists" error, if not it raises it
### Should mimic the operation of mkdir -p
### http://stackoverflow.com/questions/273192/in-python-check-if-a-directory-exists-and-create-it-if-necessary
def mkDirDashP(d):
    try:
        os.makedirs(d)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def is_number(s):
    if type(s) is not str:
        if type(s) is not int:
            return False
    try:
        float(s)
        return True
    except ValueError:
        return False


### Converts u'strings' from json.loads() to python native byte strings
### http://stackoverflow.com/questions/956867/how-to-get-string-objects-instead-of-unicode-ones-from-json-in-python
def byteify(input):
    if isinstance(input, dict):
        return {byteify(key):byteify(value) for key,value in input.iteritems()}
    elif isinstance(input, list):
        return [byteify(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input


### Schema ### 
# TODO Move this to its own file
TEXT = 'TEXT'
INTEGER = 'INTEGER'
SEP = ', '

# downloadColName = 'downloadCol'
# localDiskPathColName = 'localDiskPath'
# gameClipUrisColName = 'gameClipUris'
# screenshotUrisColName = 'screenshotUris'

clipTable = {'name' : 'clips',
            # downloadColName : gameClipUrisColName,
            'downloadCol' : 'gameClipUris',
            'primaryCol' : {
                'colName' : 'gameClipId',
                'colType' : TEXT,
                'modify' : 'PRIMARY KEY'},
            'columns' : [{
                'colName' : 'state',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'datePublished',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'dateRecorded',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'lastModified',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'userCaption',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'type',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'durationInSeconds',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'scid',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'titleId',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'rating',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'ratingCount',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'views',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'titleData',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'systemProperties',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'savedByUser',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'achievementId',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'greatestMomentId',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'thumbnails',
                'colType' : TEXT,
                'modify' : ''
            },{
                # 'colName' : gameClipUrisColName,
                'colName' : 'gameClipUris',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'xuid',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'clipName',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'titleName',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'gameClipLocale',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'clipContentAttributes',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'deviceType',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'commentCount',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'likeCount',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'shareCount',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'partialViews',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'gameClipDetails',
                'colType' : TEXT,
                'modify' : ''
            },{
                # 'colName' : localDiskPathColName,
                'colName' : 'localDiskPath',
                'colType' : TEXT,
                'modify' : 'DEFAULT NULL'
            }]
}


grabTable = {'name' : 'grabs',
            # downloadColName : screenshotUrisColName,
            'downloadCol' : 'screenshotUris',
            'primaryCol' : {
                'colName' : 'screenshotId',
                'colType' : TEXT,
                'modify' : 'PRIMARY KEY'},
            'columns' : [{
                'colName' : 'resolutionHeight',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'resolutionWidth',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'state',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'datePublished',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'dateTaken',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'lastModified',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'userCaption',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'gameClipDetails',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'type',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'scid',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'titleId',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'rating',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'ratingCount',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'views',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'titleData',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'systemProperties',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'savedByUser',
                'colType' : INTEGER,
                'modify' : ''
            },{
                'colName' : 'achievementId',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'greatestMomentId',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'thumbnails',
                'colType' : TEXT,
                'modify' : ''
            },{
                # 'colName' : screenshotUrisColName,
                'colName' : 'screenshotUris',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'xuid',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'screenshotName',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'screenshotLocale',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'screenshotContentAttributes',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'deviceType',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'screenshotDetails',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'titleName',
                'colType' : TEXT,
                'modify' : ''
            },{
                # 'colName' : localDiskPathColName,
                'colName' : 'localDiskPath',
                'colType' : TEXT,
                'modify' : 'DEFAULT NULL'
            }]
}


accountTable = {'name' : 'accounts',
            'primaryCol' : {
                'colName' : 'xuid',
                'colType' : TEXT,
                'modify' : 'PRIMARY KEY'},
            'notNullCols' : [{
                'colName' : 'gamertag',
                'colType' : TEXT,
                'modify' : 'NOT NULL'
            },{
                'colName' : 'gamertagcompare',
                'colType' : TEXT,
                'modify' : 'NOT NULL'
            }],
            'columns' : [{
                'colName' : 'firstname',
                'colType' : TEXT,
                'modify' : ''
            },{
                'colName' : 'lastname',
                'colType' : TEXT,
                'modify' : ''
            }]
}


dataTables = [clipTable, grabTable]
allTables = [clipTable, grabTable, accountTable]

# pathFinderClipIndex = {'name': 'path_finder_clips',
#                     'table' : clipTable['name'],
#                     'columns' : '(gameClipId, localDiskPath)',
#                     'where' : 'localDiskPath IS NULL'}

# pathFinderGrabIndex = {'name': 'path_finder_grabs',
#                     'table' : grabTable['name'],
#                     'columns' : '(screenshotId, localDiskPath)',
#                     'where' : 'localDiskPath IS NULL'}

# indexes = [pathFinderClipIndex, pathFinderGrabIndex]


### Respond to call from command line ###
if __name__ == "__main__":
    cwd = os.getcwd()
    
    ### Arg Parsing ###
    
    # TODO decide on and adjust to match args and parsing

    parser = argparse.ArgumentParser()
    # parser.add_argument('name', help='Name of the project (and folder) to create', nargs='?', default='_stop_')
    # parser.add_argument('-c', '--contributors', dest='contributors', help='Contributors to the project', nargs=3, action='append', metavar=('cName', 'cEmail', 'cRank'))
    parser.add_argument('-d', '--download-missing', dest='dl', help='Download missing files', action='store_true')
    parser.add_argument('-m', '--download-max', dest='dlm', help='Download a maximum number of missing files', type=int)
    parser.add_argument('-c', '--check-new', dest='c', help='Download new info from xboxapi', action='store_true')
    parser.add_argument('-u', '--user', dest='u', help='Designate xboxuserid(s) to get data for', action='append')
    parser.add_argument('-g', '--gamertag', dest='g', help='Designate gamertag(s) to get data for', action='append')
    # parser.add_argument('-i', '--info', dest='info', help='Very short description of the project')
    # parser.add_argument('-s', '--scm', dest='scm', help='Which source control management you would like initialized', choices=['git', 'None'])
    # parser.add_argument('-t', '--template', dest='template', help="Template name (also used as the name of the template's enclosing folder)", default='Generic')

    parser.add_argument('-v', '--verbose', dest='verbosity', help='Increase verbosity (off/on/firehose)', action='count', default=0)
    # parser.add_argument('dirs', help='Directories to check for duplicates', nargs='+')
    args = parser.parse_args()
    
    ### Initialize Logging ###
    if args.verbosity == 0:
        l = logging.WARNING
    elif args.verbosity == 1:
        l = logging.INFO
    else:
        l = logging.DEBUG
        
    logging.basicConfig(level=l, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.debug(str(args))

    # dirs = massageInputDirs(args.dirs)

    # checkForDupes(dirs)

    # checkDatabase(ts, indexes)
    checkDatabase(allTables)

    if args.c:
        ### GamerTags
        if args.g is not None and len(args.g) > 0:
            xids = getInfosFromGamertags(args.g)
            addAccountDetails(xids)
            for x in xids:
                getData(x['xuid'])
        ### XboxUserIds
        if args.u is not None and len(args.u) > 0:
            for u in args.u:
                getData(u)
        else:
            getData(xboxUserId)

    if args.dl:
        if args.dlm is not None:
            downloadMissingData(dataTables, args.dlm)
        else:
            downloadMissingData(dataTables)

    ### Reset working directory to original ###
    os.chdir(cwd)