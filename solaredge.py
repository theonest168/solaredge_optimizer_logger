import requests, pickle
from datetime import datetime
import json, pytz
import pandas as pd
from influxdb import DataFrameClient
from getpass import getpass
from influxdb import DataFrameClient

# Set "static" variables
login_url = "https://monitoring.solaredge.com/solaredge-apigw/api/login"
panels_url = "https://monitoring.solaredge.com/solaredge-web/p/playbackData"
DAILY_DATA = "4"
WEEKLY_DATA = "5"

# Set "customizable" variables.  Update as appropriate
COOKIEFILE = 'solaredge.cookies'
TIMEPERIOD = DAILY_DATA


SOLAREDGE_USER = input("SolarEdge username: ")
SOLAREDGE_PASS = getpass("SolarEdge password: ")
SOLAREDGE_SITE_ID = "4814191" # site id
INFLUXDB_IP = "192.168.178.198"
INFLUXDB_PORT = 8086
INFLUXDB_DATABASE = "pvmonitoring"
INFLUXDB_SERIES = "optimizers"
INFLUXDB_RETENTION_POLICY = "autogen"

session = requests.session()
try: # Make sure the cookie file exists
    with open(COOKIEFILE, 'rb') as f:
        f.close()
except IOError:  # Create the cookie file 
    session.post(login_url, headers = {"Content-Type": "application/x-www-form-urlencoded"}, data={"j_username": SOLAREDGE_USER, "j_password": SOLAREDGE_PASS})
    panels = session.post(panels_url, headers = {"Content-Type": "application/x-www-form-urlencoded", "X-CSRF-TOKEN": session.cookies["CSRF-TOKEN"]}, data={"fieldId": SOLAREDGE_SITE_ID, "timeUnit": TIMEPERIOD})
    try:
        with open(COOKIEFILE, 'wb') as f:
            pickle.dump(session.cookies, f)
            f.close()
    except IOError:
        print('Unable to create cookie file (readonly filesystem?): ' + COOKIEFILE)

try:
    with open(COOKIEFILE, 'rb') as f:
        session.cookies.update(pickle.load(f))
except IOError:
    print('Unable to open cookie file: ' + COOKIEFILE)
    print('Will continue running with new login...')
    session.post(login_url, headers = {"Content-Type": "application/x-www-form-urlencoded"}, data={"j_username": SOLAREDGE_USER, "j_password": SOLAREDGE_PASS})

# Get the cookie expiration
for cookie in session.cookies:
        if cookie.name == 'SolarEdge_SSO-1.4':
            cookie_expiration = cookie.expires
panels = session.post(panels_url, headers = {"Content-Type": "application/x-www-form-urlencoded", "X-CSRF-TOKEN": session.cookies["CSRF-TOKEN"]}, data={"fieldId": SOLAREDGE_SITE_ID, "timeUnit": TIMEPERIOD})
if (panels.status_code != 200) or (datetime.now() > datetime.fromtimestamp(cookie_expiration)):  # Update cookie if expired
    session.post(login_url, headers = {"Content-Type": "application/x-www-form-urlencoded"}, data={"j_username": SOLAREDGE_USER, "j_password": SOLAREDGE_PASS})
    panels = session.post(panels_url, headers = {"Content-Type": "application/x-www-form-urlencoded", "X-CSRF-TOKEN": session.cookies["CSRF-TOKEN"]}, data={"fieldId": SOLAREDGE_SITE_ID, "timeUnit": TIMEPERIOD})
    if (panels.status_code != 200):
        exit() # Terminate if unable to get panel data
    f.close()
    try:
        with open(COOKIEFILE, 'wb') as f:
            pickle.dump(session.cookies, f)
            f.close()
    except IOError:
        print('Unable to update expired cookie file [' + str(datetime.fromtimestamp(cookie_expiration)) + '] (readonly filesystem?): ' + COOKIEFILE)
    
response = panels.content.decode("utf-8").replace('\'', '"').replace('Array', '').replace('key', '"key"').replace('value', '"value"')
response = response.replace('timeUnit', '"timeUnit"').replace('fieldData', '"fieldData"').replace('reportersData', '"reportersData"')
response = json.loads(response)
    

#print(json.dumps(response["reportersData"], indent=2))
data = {}
for date_str in response["reportersData"].keys():
    date = datetime.strptime(date_str, '%a %b %d %H:%M:%S GMT %Y')
    date = pytz.timezone('Europe/Berlin').localize(date).astimezone(pytz.utc)
    for sid in response["reportersData"][date_str].keys():
        for entry in response["reportersData"][date_str][sid]:
            if entry["key"] not in data.keys():
                data[entry["key"]] = {}
            data[entry["key"]][date] = float(entry["value"].replace(",", ""))

df = pd.DataFrame(data)

try:
    with open("SE_keys_to_ids.pickle", 'rb') as fp:
        SE_keys_to_ids = pickle.load(fp)
except FileNotFoundError:
    SE_keys_to_ids = {}
    for module_key in df.columns:  # column names are SE keys
        print(f"get logical ID and serial nr for SE_key {module_key}")
        res = session.post(
            "https://monitoring.solaredge.com/solaredge-web/p/systemData",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-CSRF-TOKEN": session.cookies["CSRF-TOKEN"],
            },
            data=dict(
                fieldId=SOLAREDGE_SITE_ID,
                reporterId=module_key,
                activeTab="0",
                isPublic="false",
                type="panel",
            )
        )
        # res.text contains a javascript file which contains
        # as first statement ```SE.systemData = {...}```, a 
        # dictionary definition which is evaluted in the next line
        # a bit quick and dirty...
        module_dict = eval(res.text.split("\n")[11][16:-1])
        logical_id = module_dict['description'].split()[1]
        serial_nr = module_dict['serialNumber']
        SE_keys_to_ids[module_key] = (logical_id, serial_nr)
        # all information extracted, lets safe pickle file
    with open("SE_keys_to_ids.pickle", 'wb') as fp:
        pickle.dump(SE_keys_to_ids, fp)
print("Loading, resp. extracting SE_keys_to_ids completed")

# set proper df column names (replacing SE_keys by panel name)
df.columns = [ SE_keys_to_ids[mkey][0] for mkey in df.columns]

conn = DataFrameClient(INFLUXDB_IP, INFLUXDB_PORT, "", "", INFLUXDB_DATABASE)
conn.write_points(df, INFLUXDB_SERIES, retention_policy=INFLUXDB_RETENTION_POLICY)