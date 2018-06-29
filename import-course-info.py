import logging
import urllib.request
from http.client import HTTPConnection
import xmltodict
import time
import datetime
import config
import csv
#import pysftp
from smb.SMBConnection import SMBConnection

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(module)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO) #set logging format

# this method remotely calls the Acalog Courses CSV service and saves the returned file
def call_coursescsv_export(_catalog_id):
    # service URL
    csv_url = config.acalog_rs_coursescsv_url.format(_catalog_id)

    # URL call requires basic authentication so need to set up a password manager
    password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    password_mgr.add_password(None, config.acalog_rs_toplevel_url, config.acalog_rs_username,config.acalog_rs_password)
    handler = urllib.request.HTTPBasicAuthHandler(password_mgr)

    # create "opener" (OpenerDirector instance)
    opener = urllib.request.build_opener(handler)

    try:
        # use the opener to fetch a URL
        u = opener.open(csv_url)

        # check the content type
        content_type = u.getheader('Content-Type')
        #print(u.headers.as_string())   
        if 'text/csv' in content_type:
            logger.info("Received valid courses csv file, so will save and process.")
            # it's the actual file export so download and process 
            with open('courses.csv', 'wb') as f:
                response = u.read()
                f.write(response)
            
            # and then we'd process the file here
        else:
            #it's the dumb processing message
            #so we'll sleep for a while and try again
            logger.info("Received the dumb processing message from Acalog, so we'll sleep and try to get the actual courses CSV in a bit.")
            time.sleep(1200)    # sleepy time
            call_coursescsv_export(_catalog_id) # re-call the service now that we've slept on it

    except Exception as e:
        logger.error("Error in call_coursescsv_export. " + str(e))

def put_file_smb(_filename):

    try:
        # create samba connection
        conn = SMBConnection(config.smb_username,config.smb_password,config.smb_localname,config.smb_remotename,'',use_ntlm_v2=True,
                            sign_options=SMBConnection.SIGN_WHEN_SUPPORTED,
                            is_direct_tcp=True)
        connected = conn.connect(config.smb_remotename,445)

        # save file to share location
        try:
            with open(_filename, 'rb') as fstore:
                conn.storeFile(config.smb_sharename, config.smb_filename, fstore)

            #Response = conn.listShares(timeout=30)  # obtain a list of shares
            #print('Shares on: ' + config.smb_remotename)

            #for i in range(len(Response)):  # iterate through the list of shares
            #    print("  Share[",i,"] =", Response[i].name)

        except Exception as e:
            logger.exception("Error storing file on remote share. " + str(e))
    except Exception as e:
        logger.exception("Error establinshing samba connection. " + str(e))
    finally:
        conn.close()

'''
# completely untested sftp file movement option
def put_file_sftp(_filename):

    try:
        with pysftp.Connection(config.sftp_host, username=config.sftp_user, password=config.sftp_password) as sftp:
            with sftp.cd(config.sftp_directory):  # temporarily chdir to public
                sftp.put(_filename)  # upload file to public/ on remote
    except Exception as e:
        logger.exception("Error sftping file to server. " + str(e))
'''

if __name__ == '__main__':

    HTTPConnection.debuglevel = 1  

    # use Acalog API to get ID of current publish, non-archived catalog
    catalog_xml = urllib.request.urlopen(config.acalog_ws_apiurl.format(config.acalog_ws_apikey))
    #print(catalog_xml.headers.as_string())
    xmldict = xmltodict.parse(catalog_xml)
    catalog_id = 2  #default value

    #parse returned xml for cat id
    # catalog info may be OrderedDict or list, so process accordingly
    if isinstance(xmldict['catalogs']['catalog'], dict):
        catdict = dict(xmldict['catalogs']['catalog']) # use regular dict, not OrderedDict

        state = catdict['state']
        if state['published'] == 'Yes' and state['archived'] == 'No':
            catalog_id = catdict['@id'].split('-')[-1]
    else:
        catalogs_dict = xmldict['catalogs']['catalog']

        for catdict in catalogs_dict:
            #print(catdict)
            state = catdict['state']
            if state['published'] == 'Yes' and state['archived'] == 'No':

                catalog_id = catdict['@id'].split('-')[-1]
                break
    #print(catalog_id)

    # now use the catalog ID to pull the courses CSV
    #call_coursescsv_export(catalog_id)
    call_coursescsv_export(2)

    # now that we have a full courses file, process it to just get the pieces we need
    today = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    fname_in = 'courses.csv'
    fname_out = "courses_{}.csv".format(today)
    file_success = False

    with open(fname_in, 'r', encoding='utf8') as fin, open(fname_out, 'w') as fout:
        reader = csv.DictReader(fin)
        next(reader, None)  # skip the headers
        writer = csv.writer(fout)

        try:
            for row in reader:
                writer.writerow( (row["Prefix"]+row["Common Course Identifier"],row["Code"],row["Catalog Name"],row["Course Outcomes"]) )
            
            file_success = True
        except csv.Error as e:
            logger.exception("Error reading or writing courses file. " + str(e))

    # if we successfully generated the file, move it to its remote destination
    if file_success:
        put_file_smb(fname_out)