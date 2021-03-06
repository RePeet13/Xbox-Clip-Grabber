import apprise, argparse, datetime, errno, inspect, json, logging, os, pprint, sqlite3, sys, urllib
# Import subfolder modules
# from http://stackoverflow.com/questions/279237/import-a-module-from-a-relative-path
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"lib")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)
import progressbar

# fullSlackXboxClipsWebhookUrl = https://hooks.slack.com/services/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
appriseSlackUrl = 'slack://xbox-clip-notifier@T235VRDJ5/BNG1G1W8J/vmDeT05zpMIfoRFibhks6UaH/#xbox-clips'

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
dbVersion = 2 # Version 2: 11/30/19

basePath = 'data' # relative to script location
clipsPath = 'clips'
grabsPath = 'grabs'

### Path where this script resides ###
def getScriptPath():
    return os.path.dirname(os.path.realpath(__file__))


def getCountsAsJson():
    path = os.path.join(getScriptPath(), basePath)
    gamers = [f for f in os.listdir(path) if not os.path.isfile(os.path.join(path, f))]
    gamers.remove('Unversioned')
    counts = {}
    for gamer in gamers:
        counts[gamer] = {'clips' : 0}
        gamerPath = os.path.join(path, gamer)
        for game in [f for f in os.listdir(gamerPath) if not os.path.isfile(os.path.join(gamerPath, f))]:
            counts[gamer][game + ' - clips'] = len([f for f in os.listdir(os.path.join(gamerPath, game)) if os.path.isfile(os.path.join(gamerPath, game, f)) and 'mp4' in f])
            counts[gamer][game + ' - grabs'] = len([f for f in os.listdir(os.path.join(gamerPath, game)) if os.path.isfile(os.path.join(gamerPath, game, f)) and 'png' in f])
            counts[gamer]['clips'] = counts[gamer]['clips'] + counts[gamer][game + ' - clips']
#    pprint.pprint(counts)
    print(counts)

def getData(xboxId):
    logging.info('Getting all data')
    # TODO decide what is going to be returned by below functions (stats on what was added, failed, etc)

    checkForXboxId(xboxId)

    result = []
    result.append(getClips(xboxId))
    result.append(getGrabs(xboxId))
    return result

### url: url to hit, pages: how many pages to get, -1 for all
def getDataFromUrl(url, pages):
    req = getReq(url)
    response = urllib.request.urlopen(req)
    data = byteify(json.loads(response.read()))
#    pprint.pprint(data)
    if pages < 0: # get all pages
        while 'x-continuation-token' in dict(response.info()):
            logging.debug('Getting another page')
            newurl = url + '?continuationToken=' + dict(response.info())['x-continuation-token']
            req = getReq(newurl)
            response = urllib.request.urlopen(req)
            data = data + byteify(json.loads(response.read()))
    return data


def getClips(xboxId):
    logging.info('Getting all the clips')
    url = xboxApiBase + xboxId + '/' + clipsUrl
    data = getDataFromUrl(url, -1)
    result = addListToDb(data)

    return result # TODO change this to be more helpful (success true/false, error, etc)


def getGrabs(xboxId):
    logging.info('Getting all the grabs')

    url = xboxApiBase + xboxId + '/' + grabsUrl
    data = getDataFromUrl(url, -1)
    # pprint.pprint(data)
    result = addListToDb(data)

    return result # TODO change this to be more helpful (success true/false, error, etc)


def checkForXboxId(xuid):
    con = getDb()
    c = con.cursor()

    logging.info('Checking for local details for xuid: ' + str(xuid))

    s = "SELECT {gt} FROM {at} WHERE {xidn}={xid}"\
        .format(gt='gamertag', at=accountTable['name'], \
            xidn=accountTable['primaryCol']['colName'], xid=xuid)
    logging.debug('Statement is: \n\t' + s)
    c.execute(s)
    id_exists = c.fetchone()[0]

    con.commit()
    con.close()

    if not id_exists:
        logging.debug('Account not found, attempting population')
        infoResponse = getInfosFromXuids([xuid])
        for a in infoResponse:
            if a['success']:
                addAccountDetails(a)

    return id_exists


def addAccountDetails(infos):
    # TODO play with other endpoints (like profile or gamercard might be useful)
    con = getDb()
    c = con.cursor()
    for i in infos:
        logging.debug('All input info ' + str(i))
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
        response = urllib.request.urlopen(req)
        res = wrapHttpResponse(byteify(json.loads(response.read())))
        logging.debug(res)
        if res['success'] is True:
            gts.append({
                'success' : True,
                'xuid' : str(xid),
                'gamertag' : res['response'],
                'response' : res
                })
                # TODO have some validation around this, maybe check http code
        else:
            logging.error(res)
            gts.append({
                'success' : False,
                'xuid' : str(xid),
                'gamertag' : '',
                'response' : res
                })

    return gts


def getInfosFromGamertags(gts):
    logging.info('Getting xuids from gamertags')

    xuids = []
    for g in gts:
        url = xboxApiBase + getXuidUrl + g
        req = getReq(url)
        response = urllib.request.urlopen(req)
        res = wrapHttpResponse(byteify(json.loads(response.read())))
        logging.debug(res)
        if res['success']:
            xuids.append({
                'success' : True,
                'xuid' : str(res['response']),
                'gamertag' : g,
                'response' : res
                })
                # TODO have some validation around this, maybe check http code
        else:
            logging.error(res)
            xuids.append({
                'success' : False,
                'xuid' : '',
                'gamertag' : g,
                'response' : res
                })

    return xuids


### Wrapper to add a list of items to the db
def addListToDb(l):
    logging.info('Adding list to database')
    logging.debug('Adding ' + str(len(l)) + ' items to the db')
    con = getDb()
    c = con.cursor()

    bar = progressbar.ProgressBar()
    res = []
#    pprint.pprint(l)
    try:
        logging.debug('List to add to db: ' + str([x[clipTable['primaryCol']['colName']] for x in l]))
    except KeyError:
        logging.debug('List to add to db: ' + str([x[grabTable['primaryCol']['colName']] for x in l]))
    for listItem in bar(l):
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

def doNotify(person, numRows):
    # Pass on this after adding apprise for slack notification
    pass
    # logging.warning('Notifying now')
    # con = getDb()
    # c = con.cursor()

    # # TODO add logic to have custom notification preferences (aka how to notify)
    # s = "SELECT pbkey FROM {tn} WHERE xuid = '{x}'".format(tn=accountTable['name'], x=person['xuid'])
    # logging.debug('Statement for getting pbkey to notify is:\n' + s)
    # c.execute(s)
    # # TODO need error handling if apikey doesnt exist, looks like: (None,)
    # apikey = c.fetchone()[0]
    # logging.debug(apikey)
    # if apikey is not None:
    #     sendNote({'title' : 'Clip notification',
    #         'body' : 'You have ' + str(numRows) + ' undownloaded clips on Xbox'}, apikey)

    # con.commit()
    # con.close()


def sendNote(data, apikey):
    values = {'type' : 'note',
                'title' : data['title'],
                'body' : data['body']}
    pushurl = 'https://api.pushbullet.com/v2/pushes'
    auth = 'Basic ' + apikey.encode('base64').rstrip()

    data = urllib.urlencode(values)
    req = urllib.request.Request(pushurl)
    req.add_header('Authorization', auth)
    req.add_data(data)
    res = urllib.request.urlopen(req)


def checkForMissingData(inTables, dl, xuid=False, notif=False, maxNum=float("inf")):

    con = getDb()
    c = con.cursor()
    counter = 1

    for t in inTables:
        if counter > maxNum:
            logging.info('Counter reached max: ' + str(counter))
            continue

        logging.info('Looking at database table: ' + t['name'])
        logging.debug('Grabbing candidates')
        selArr = [t['primaryCol']['colName'], 'titleName', 'deviceType', t['downloadCol'], 'datePublished', 'xuid', v1AddCols[0]['colName']]
        s = "SELECT {sel} FROM {tn} WHERE (({cn} = NULL) OR ({cn} IS NULL)) AND ({tried} < 3 OR {tried} IS NULL)"\
            .format(sel=SEP.join(selArr), tn=t['name'], cn='localDiskPath', tried=v1AddCols[0]['colName']) # TODO this and below shouldnt be hardcoded really
        if xuid:
            s = s + " AND xuid = '" + xuid['xuid'] + "'"
        logging.debug('Statement is: \n\t' + s)
        c.execute(s)

        all_rows = c.fetchall()
        logging.debug('All rows: Length - ' + str(len(all_rows)) + '\n' + str(all_rows))

    # TODO add notified column
        if len(all_rows):
            print ('Looks like there are ' + str(len(all_rows)) + ' ' + t['name'] + ' missing from the local filesystem')
            if notif and xuid:
                doNotify(xuid, len(all_rows))
            if dl:
                counter = downloadMissingData(t, all_rows, counter, maxNum)
        else:
            print (t['name'] + ':\tLooks like the local filesystem is up to date!')

    con.commit()
    con.close()


def downloadMissingData(t, all_rows, counter, maxNum=float("inf")):
    logging.info('Getting ready to download all missing data (clips/grabs)')

    con = getDb()
    c = con.cursor()
        
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
        # TODO should probably think about a directory/filename safe string converter
        d = os.path.join(d, r[1].replace(':','-'))
        # d = d.encode('ascii', 'ignore')
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
            # TODO should be checking the expiration of the link, and marking it as tried/expired if it doesn't work, then ignore it in the 'missing' query (new column for 'skip')
            logging.debug('URI to be downloaded is:\n' + str(v))
            dateString = r[4][:19].replace(':','')
            fn = dateString + '_' + r[0][:7] + '_' + str(cnt)
            # TODO parse uri for this instead of hardcode
            if t['name'] is 'clips':
                fn = fn + '.mp4'
            elif t['name'] is 'grabs':
                fn = fn + '.png'
            fn = os.path.join(d, fn)
            success = downloadFile(v['uri'], fn)

            cnt += 1
            if success:
                totalSuccess = True
                downed.append(fn)
                counter += 1

        if totalSuccess:
            # TODO this should probably be done whether or not total success happened (because we want to record the ones we did get)
            # Add back the paths to the db
            logging.debug('Adding file paths back to db')
            s = "UPDATE {tn} SET {c}=('{p}'),createdDate=DATETIME('now') WHERE {idf}=('{id}')"\
                .format(tn=t['name'], c='localDiskPath', p=','.join(downed), \
                    idf=t['primaryCol']['colName'], id=r[0])
            logging.debug('Statement is: \n\t' + s)
            c.execute(s)
        else:
            logging.debug('File download tried and failed')
            tries = 0 if r[6] is None else (r[6]+1)
            s = "UPDATE {tn} SET {c}={p} WHERE {idf}=('{id}')"\
                .format(tn=t['name'], c=v1AddCols[0]['colName'], p=tries, \
                    idf=t['primaryCol']['colName'], id=r[0])
            logging.debug('Statement is: \n\t' + s)
            c.execute(s)


    con.commit()
    con.close()

    return counter


### Initially http://stackoverflow.com/a/22776/286994
### Then http://blog.radevic.com/2012/07/python-download-url-to-file-with.html
def downloadFile(url, file_name):
    # url = url.encode('ascii', 'ignore')
    # file_name = file_name.encode('ascii', 'ignore')
    try:
        req = getReq(url)
        logging.info('Downloading: {0} \n\tUrl: {1}'.format(file_name, url))
        u = urllib.request.urlopen(req)
        f = open(file_name, 'wb')
        file_size = int(u.getheader("Content-Length"))
        logging.debug('Download response info: \n\t' + str(u.info()))
        arr = file_name.split(os.sep)
        shortfn = arr[-1]
        game = arr[-2]

        file_size_dl = 0
        block_sz = 8192
        
        bar = progressbar.DataTransferBar(max_value=file_size).start()

        while True:
            buffer = u.read(block_sz)
            if not buffer:
                break

            file_size_dl += len(buffer)
            f.write(buffer)
            bar.update(file_size_dl)
            # status = r"%s/%s - %10d  [%3.2f%%]" % (game, shortfn, file_size_dl, file_size_dl * 100. / file_size)
            # status = status + chr(8)*(len(status)+1)
            # sys.stdout.write(status)

        f.close()
        bar.finish()
        # sys.stdout.write('\n')
        logging.debug('Download Success')
        return True
    except urllib.error.HTTPError as e:
        # TODO handle errors here
        # mark as errored in the db
        logging.error('Error while downloading the file: {0}\n{1}'.format(e.errno, e.strerror))
        return False
    except UnicodeEncodeError as e:
        # logging.error('Unicode error: {0}\n{1}'.format(e.errno, e.strerror))
        logging.error('Annoyance in FileName:\n' + str(file_name))
        logging.error('Or in url:\n' + str(url))
        return False


def getReq(url):
    # TODO implement with headers from above
    req = urllib.request.Request(url)
    req.add_header('X-AUTH', xboxApiKey)
    return req


def setName(idOrGt, whichName, name1):
  # TODO as optional variable to take in both parts
    pass

def doNotifySlackWithNew(notifier):
    logging.info('Notifying slack now')
    con = getDb()
    c = con.cursor()

    whoAndWhat = {}
    s = "SELECT xuid, gameClipId, localDiskPath FROM {tn} WHERE createdDate >= Datetime('now', '-1 day')".format(tn='clips')
    logging.debug('Statement for getting stuff to notify is:\n' + s)
    c.execute(s)
    # TODO need error handling if apikey doesnt exist, looks like: (None,)
    results = c.fetchall()
    logging.debug(results)
    logging.debug(len(results))

    con.commit()
    con.close()

    splitter = '/media/data2/git/Xbox-Clip-Grabber/data/'
    baseUrl = 'https://xbox.tpeet.net/'

    for row in results:
        gt = checkForXboxId(row[0])
        if row[2]:
            if gt not in whoAndWhat: # xuid of clip
                whoAndWhat[gt] = []

            url = baseUrl + urllib.parse.quote(row[2].split(splitter)[1])
            whoAndWhat[gt].append(url)

    print(whoAndWhat)
    for gt in whoAndWhat:
        for url in whoAndWhat[gt]:
            notifier.notify(
                title=gt + ' clip!',
                body=url
            )
    


### Check the schema of the database and open a new one if necessary
### http://sebastianraschka.com/Articles/2014_sqlite_in_python_tutorial.html (this method and below)
# def checkDatabase(inTables, inIndexes):
def checkDatabase(inTables):
    logging.info('Checking db consistency')

    # TODO check if database actually exists at all?

    con = getDb()
    c = con.cursor()

    s = 'PRAGMA user_version'
    c.execute(s)
    v = c.fetchone()[0]
    logging.info('Database is version: ' + str(v))
    
    if v < dbVersion:
        dbVersionUpgrade(con, c)
    # TODO need to reconsider this strategy (since the dbs are being upgraded before checking if anything is missing)
    # need to change this because when there is no db, it tries to alter a table that is not there (and fails)

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
                    logging.debug('Attempting add for column: ' + col)
                    s = "ALTER TABLE {tn} ADD COLUMN '{nf}' {ft} {p}"\
                        .format(tn=t['name'], nf=col['colName'], ft=col['colType'], p=col['modify'])
                    logging.debug('Statement is: ' + s)
                    c.execute(s)
            except:
                logging.debug('There was an exception adding a missing column, not blowing it away')
                # TODO need a better solution than this...

                # c.execute('DROP TABLE {}'.format(t['name']))
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


###  Upgrade db from original
def dbVersionUpgrade(con, c):
    logging.warning('Upgrading db version')

    # Check that it actually is version 0
    s = 'PRAGMA user_version'
    c.execute(s)
    v = c.fetchone()[0]
    logging.debug('In V1 upgrade script, database is version: ' + str(v))

    if not v:
        # Do version upgrade from nothing to 1
        for col in v1AddCols:
            for t in col['tableName']:
                s = "ALTER TABLE {tn} ADD COLUMN '{nf}' {ft} {p}"\
                        .format(tn=t, nf=col['colName'], ft=col['colType'], p=col['modify'])
                logging.debug('Statement is: \n\t' + s)
                c.execute(s)

        s = 'PRAGMA user_version = 1'
        c.execute(s)
        logging.info('Database version 1 upgrade is complete')

        con.commit()
    elif v is 1:
        # Do version upgrade from 1 to 2
        for col in v2AddCols:
            for t in col['tableName']:
                s = "ALTER TABLE {tn} ADD COLUMN '{nf}' {ft} {p}"\
                        .format(tn=t, nf=col['colName'], ft=col['colType'], p=col['modify'])
                logging.debug('Statement is: \n\t' + s)
                c.execute(s)

        s = 'PRAGMA user_version = 2'
        c.execute(s)
        logging.info('Database version 2 upgrade is complete')

        con.commit()
    elif v > 1:
        logging.error('In V1 upgrade script, but db is already at/past V1')
        return
    # TODO if version 2 is out, then move on to that upgrade script 
    

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
    return input
    # if isinstance(input, dict):
    #     return {byteify(key):byteify(value) for key,value in input.items()}
    # elif isinstance(input, list):
    #     return [byteify(element) for element in input]
    # elif isinstance(input, str):
    #     return input.encode('utf-8')
    # else:
    #     return input


def wrapHttpResponse(res):
    logging.debug(res)
    try:
        s = res['success']
    except TypeError:
        return {
            'success' : True,
            'response': res
        }
    except KeyError:
        return {
            'success' : True,
            'response': res
        }


### Schema ### 
# These are all Version 0
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
            },{
                'colName' : 'notes',
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
            },{
                'colName' : 'notes',
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
            },{
                'colName' : 'pbkey',
                'colType' : TEXT,
                'modify' : ''
            }]
}

### Columns to add on V1 db upgrade
v1AddCols = [{ # Columns to be added in V1
            'colName' : 'timesAttempted',
            'colType' : INTEGER,
            'modify' : '',
            'tableName' :  ['clips','grabs']
        }]

### Columns to add on V2 db upgrade
v2AddCols = [{ # Columns to be added in V1
            'colName' : 'createdDate',
            'colType' : TEXT,
            'modify' : '',
            'tableName' :  ['clips','grabs']
        }]

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
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--download-missing', dest='dl', help='Download missing files', action='store_true')
    parser.add_argument('-m', '--download-max', dest='dlm', help='Download a maximum number of missing files', type=int)
    parser.add_argument('-c', '--check-new', dest='c', help='Download new info from xboxapi', action='store_true')
    parser.add_argument('-u', '--user', dest='u', help='Designate xboxuserid(s) to get data for', action='append')
    parser.add_argument('-g', '--gamertag', dest='g', help='Designate gamertag(s) to get data for', action='append')
    parser.add_argument('-n', '--notify', dest='n', help='Notify of undownloaded clips/grabs', action='store_true')
    parser.add_argument('-j', '--json', dest='j', help='Counts of gamer clips/grabs', action='store_true')
    parser.add_argument('-t', '--test', dest='t', help='Just run the test path', action='store_true')

    parser.add_argument('-v', '--verbose', dest='verbosity', help='Increase verbosity (off/on/firehose)', action='count', default=0)
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


    checkDatabase(allTables)

    ### Initialize Notifications
    notifySlack = apprise.Apprise()
    notifySlack.add(appriseSlackUrl)
    
    if args.t:
        logging.debug('Test Path Enabled')
        exit()

    if args.c:
        ### GamerTags
        if args.g is not None and len(args.g) > 0:
            xids = getInfosFromGamertags(args.g)
            for a in xids:
                if a['success']:
                    logging.debug('success')
                    logging.debug(a)
                    print(a['gamertag'])
                    addAccountDetails([a])
                    getData(a['xuid'])
                    checkForMissingData(dataTables, False, a, notif=args.n)

        ### XboxUserIds
        if args.u is not None and len(args.u) > 0:
            for u in args.u:
                getData(u)
        else:
            pass
            #getData(xboxUserId)
            # Hack that auto pulls my clips and stuff (should remove i guess)


    if args.dl:
        if args.dlm is not None:
            checkForMissingData(dataTables, True, False, notif=args.n, maxNum=args.dlm)
        else:
            checkForMissingData(dataTables, True, False, notif=args.n)

    if args.j:
        getCountsAsJson()

    if args.n:
        doNotifySlackWithNew(notifySlack)

    ### Reset working directory to original ###
    os.chdir(cwd)
