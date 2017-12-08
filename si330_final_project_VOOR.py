import petl as etl
import sqlite3
import requests
import json
import time
import ast

#####################################
# John Voorhess SI330 Final Project #
#####################################
ONBOARD_API_KEY = "7a293014fc9cf968c31c53eb46916385"
HEADERS = {'apikey': ONBOARD_API_KEY, 'accept': 'application/json'}
BASE_URL = "https://search.onboard-apis.com/propertyapi/v1.0.0/saleshistory/detail?address1="
BASE_URL_ASSESSMENT = "https://search.onboard-apis.com/propertyapi/v1.0.0/assessment/detail?address1="
GOOGLE_MAPS_API_KEY = "AIzaSyDBwwT00ktZRGAnwc2lNKwJRxVMvVdo4IE"

# Open a connection to the database
conn = sqlite3.connect('drug_labs.db')

# # create a petl object for the contents of the csv file
# mi_labs = etl.fromcsv('mi_drug_labs.csv')
#
# # print the top of the petl object to verify contents
# print(mi_labs.head())
#
# # drop previous version of lab table
# conn.execute("DROP TABLE IF EXISTS lab;")
# # drop previous version of lab_address_data table:
# conn.execute("DROP TABLE IF EXISTS lab_address_data")
#
# # store sql statement to create lab table with unique integer primary key in a variable
# create_lab = """
# CREATE TABLE IF NOT EXISTS lab (
# state char(64),
# county char(128),
# city char(128),
# address char(128),
# date_discovered char(64)
# )
# """
#
# create_lab_address_data = """
# CREATE TABLE IF NOT EXISTS lab_address_data (
# address varchar,
# json_data varchar
# )
# """

# create_lab_address_gps = """
# CREATE TABLE IF NOT EXISTS lab_address_GPS (
# address varchar,
# city varchar,
# state varchar,
# json_data varchar
# )
# """
# conn.execute(create_lab_address_gps)




#
# # run create statement to create lab table
# conn.execute(create_lab)
# conn.execute(create_lab_address_data)
#
# # populate lab table from previously instantiated petl object
# mi_labs.todb(conn,'lab')

# check number of records in table
count = conn.execute("SELECT count(*) FROM lab;")
print(count.fetchone())

count = conn.execute("SELECT count(*) FROM lab_address_GPS;")
print(count.fetchone())

address = conn.execute("SELECT address, city, state FROM lab;")
addresses = address.fetchall()
# for address in addresses:
#     print(address)



# query google API for GPS coords and insert them into GPS coord table
# for address in addresses:
#     response = requests.get("https://maps.googleapis.com/maps/api/geocode/json?address="+address[0]+",+"+address[1]+",+"+address[2]+"&key="+GOOGLE_MAPS_API_KEY)
#     time.sleep(.025)
#     json_data = json.loads(response.text)
#     #print(json_data)
#     json_string_dirty = str(json_data)
#     json_string = json_string_dirty.replace("'","`")
#     print(json_string)
#
#     conn.execute("INSERT INTO lab_address_GPS (address, city, state, json_data) VALUES ('"+ address[0] +"', '"+ address[1] +"', '"+ address[2] +"', '"+json_string+"');")
#     conn.commit()


# sort occurence of cities to choose groups to analyze
# results indicate highest number of occurences on West side of state.
# choosing one city on West side and one on East side with similar number of occurences.
# chose Battle Creek (27) and Port Huron (22)
city_count_result = conn.execute("SELECT city, count(*) from lab GROUP BY city ORDER BY count(*) DESC;")
city_count = city_count_result.fetchall()
print(city_count)

# query Battle Creek addresses:
battle_creek_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city = 'BATTLE CREEK';")
battle_creek_addresses = battle_creek_address_result.fetchall()

battle_creek_address_tuple_list = []
for address in battle_creek_addresses:
    address_1 = address[0]
    address_2 = address[1]+", "+address[2]
    battle_creek_address_tuple_list.append((address_1,address_2))

print("Number of Battle Creek address tuples- "+str(len(battle_creek_address_tuple_list)))

port_huron_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city = 'PORT HURON';")
port_huron_addresses = port_huron_address_result.fetchall()

port_huron_address_tuple_list = []
for address in port_huron_addresses:
    address_1 = address[0]
    address_2 = address[1]+", "+address[2]
    port_huron_address_tuple_list.append((address_1,address_2))

print("Number of Port Huron address tuples- "+str(len(port_huron_address_tuple_list)))

jackson_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city = 'JACKSON';")
jackson_addresses = jackson_address_result.fetchall()

jackson_address_tuple_list = []
for address in jackson_addresses:
    address_1 = address[0]
    address_2 = address[1]+", "+address[2]
    jackson_address_tuple_list.append((address_1,address_2))

print("Number of Jackson address tuples- "+str(len(jackson_address_tuple_list)))

allegan_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city = 'ALLEGAN';")
allegan_addresses = allegan_address_result.fetchall()

allegan_address_tuple_list = []
for address in allegan_addresses:
    address_1 = address[0]
    address_2 = address[1]+", "+address[2]
    allegan_address_tuple_list.append((address_1,address_2))

print("Number of Allegan address tuples- "+str(len(allegan_address_tuple_list)))

kalamazoo_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city = 'KALAMAZOO';")
kalamazoo_addresses = kalamazoo_address_result.fetchall()

kalamazoo_address_tuple_list = []
for address in kalamazoo_addresses:
    address_1 = address[0]
    address_2 = address[1]+", "+address[2]
    kalamazoo_address_tuple_list.append((address_1,address_2))

print("Number of Kalamazoo address tuples- "+str(len(kalamazoo_address_tuple_list)))

thursday_address_tuple_list = []
thursday_city_list = ['PORTAGE','THREE RIVERS','FLINT','STURGIS','PAW PAW','COLDWATER','OTSEGO','PLAINWELL','LANSING','OWOSSO','VICKSBURG','DOWAGIAC','GALESBURG','GOBLES','HARTFORD','CHARLOTTE','DECATUR','HASTINGS','LAWTON','NILES','DELTON','WHITE PIGEON','HOLLAND','BENTON HARBOR','CONSTANTINE','BRONSON','MARCELLUS','MATTAWAN','CASSOPOLIS','COLOMA','EDWARDSBURG','HILLSDALE']
for thursday_city in thursday_city_list:
    thursday_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city = '"+thursday_city+"';")
    thursday_addresses = thursday_address_result.fetchall()
    for address in thursday_addresses:
        address_1 = address[0]
        address_2 = address[1]+", "+address[2]
        thursday_address_tuple_list.append((address_1,address_2))
print("Number of thursday address tuples- "+str(len(thursday_address_tuple_list)))

list_to_append = ['KALAMAZOO','ALLEGAN','JACKSON','PORT HURON','BATTLE CREEK']
already_in = list_to_append + thursday_city_list
print(already_in)

friday_address_tuple_list = []
friday_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city NOT IN ('KALAMAZOO', 'ALLEGAN', 'JACKSON', 'PORT HURON', 'BATTLE CREEK', 'PORTAGE', 'THREE RIVERS', 'FLINT', 'STURGIS', 'PAW PAW', 'COLDWATER', 'OTSEGO', 'PLAINWELL', 'LANSING', 'OWOSSO', 'VICKSBURG', 'DOWAGIAC', 'GALESBURG', 'GOBLES', 'HARTFORD', 'CHARLOTTE', 'DECATUR', 'HASTINGS', 'LAWTON', 'NILES', 'DELTON', 'WHITE PIGEON', 'HOLLAND', 'BENTON HARBOR', 'CONSTANTINE', 'BRONSON', 'MARCELLUS', 'MATTAWAN', 'CASSOPOLIS', 'COLOMA', 'EDWARDSBURG', 'HILLSDALE');")
friday_addresses = friday_address_result.fetchall()
for address in friday_addresses:
    address_1 = address[0]
    address_2 = address[1]+", "+address[2]
    friday_address_tuple_list.append((address_1,address_2))
print("Number of friday address tuples- "+str(len(friday_address_tuple_list)))

# for address_tuple in thursday_address_tuple_list:
#     print(address_tuple[0] + ", " + address_tuple[1])


# for address_tuple in port_huron_address_tuple_list:
#     print(address_tuple[0] + ", " + address_tuple[1])
#
# for address_tuple in battle_creek_address_tuple_list:
#     print(address_tuple[0] + ", " + address_tuple[1])
#
# for address_tuple in jackson_address_tuple_list:
#     print(address_tuple[0] + ", " + address_tuple[1])
#
# for address_tuple in allegan_address_tuple_list:
#     print(address_tuple[0] + ", " + address_tuple[1])

#query api for all thursday addresses:
# for address_tuple in thursday_address_tuple_list:
#     response = requests.get(BASE_URL_ASSESSMENT + address_tuple[0] + "&address2=" + address_tuple[1], headers=HEADERS)
#     time.sleep(7)
#     json_data = json.loads(response.text)
#     print(json_data)
#     json_string_dirty = str(json_data)
#     json_string = json_string_dirty.replace("'","`")
#     print(json_string)
#
#     coupled_address_tuple = address_tuple[0]+", "+address_tuple[1]
#     print(coupled_address_tuple)
#     #try:
#
#     conn.execute("INSERT INTO lab_address_data (address, json_data) VALUES ('"+ coupled_address_tuple +"', '"+ json_string +"');")


#query api for all Battle Creek addresses:
# for address_tuple in battle_creek_address_tuple_list:
#     response = requests.get(BASE_URL_ASSESSMENT + address_tuple[0] + "&address2=" + address_tuple[1], headers=HEADERS)
#     time.sleep(7)
#     json_data = json.loads(response.text)
#     print(json_data)
#     json_string_dirty = str(json_data)
#     json_string = json_string_dirty.replace("'","`")
#     print(json_string)
#
#     coupled_address_tuple = address_tuple[0]+", "+address_tuple[1]
#     print(coupled_address_tuple)
#     #try:
#
#     conn.execute("INSERT INTO lab_address_data (address, json_data) VALUES ('"+ coupled_address_tuple +"', '"+ json_string +"');")

    # except:
    #     print("nothing inserted for " + address_tuple[0] + ", " + address_tuple[1])

# query api for all port huron addresses:
# for address_tuple in port_huron_address_tuple_list:
#     response = requests.get(BASE_URL_ASSESSMENT + address_tuple[0] + "&address2=" + address_tuple[1], headers=HEADERS)
#     time.sleep(7)
#     json_data = json.loads(response.text)
#     print(json_data)
#     json_string_dirty = str(json_data)
#     json_string = json_string_dirty.replace("'","`")
#     print(json_string)
#
#     coupled_address_tuple = address_tuple[0]+", "+address_tuple[1]
#     print(coupled_address_tuple)
#     #try:
#
#     conn.execute("INSERT INTO lab_address_data (address, json_data) VALUES ('"+ coupled_address_tuple +"', '"+ json_string +"');")

#query api for all Jackson addresses:
# for address_tuple in jackson_address_tuple_list:
#     response = requests.get(BASE_URL_ASSESSMENT + address_tuple[0] + "&address2=" + address_tuple[1], headers=HEADERS)
#     time.sleep(7)
#     json_data = json.loads(response.text)
#     print(json_data)
#     json_string_dirty = str(json_data)
#     json_string = json_string_dirty.replace("'","`")
#     print(json_string)
#
#     coupled_address_tuple = address_tuple[0]+", "+address_tuple[1]
#     print(coupled_address_tuple)
#     #try:
#
#     conn.execute("INSERT INTO lab_address_data (address, json_data) VALUES ('"+ coupled_address_tuple +"', '"+ json_string +"');")

#query api for all Allegan addresses:
# for address_tuple in allegan_address_tuple_list:
#     response = requests.get(BASE_URL_ASSESSMENT + address_tuple[0] + "&address2=" + address_tuple[1], headers=HEADERS)
#     time.sleep(7)
#     json_data = json.loads(response.text)
#     print(json_data)
#     json_string_dirty = str(json_data)
#     json_string = json_string_dirty.replace("'","`")
#     print(json_string)
#
#     coupled_address_tuple = address_tuple[0]+", "+address_tuple[1]
#     print(coupled_address_tuple)
#     #try:
#
#     conn.execute("INSERT INTO lab_address_data (address, json_data) VALUES ('"+ coupled_address_tuple +"', '"+ json_string +"');")

#query api for all Kalamazoo addresses:
# for address_tuple in kalamazoo_address_tuple_list:
#     response = requests.get(BASE_URL_ASSESSMENT + address_tuple[0] + "&address2=" + address_tuple[1], headers=HEADERS)
#     time.sleep(7)
#     json_data = json.loads(response.text)
#     print(json_data)
#     json_string_dirty = str(json_data)
#     json_string = json_string_dirty.replace("'","`")
#     print(json_string)
#
#     coupled_address_tuple = address_tuple[0]+", "+address_tuple[1]
#     print(coupled_address_tuple)
#     #try:
#
#     conn.execute("INSERT INTO lab_address_data (address, json_data) VALUES ('"+ coupled_address_tuple +"', '"+ json_string +"');")


# make list to hold ROWID of bad records in lab_address_data
rowid_to_delete = []

# find records in database where address was not found
bad_result_list = []
bad_geocoder_result = conn.execute("SELECT lab_address_data.ROWID FROM lab_address_data WHERE json_data = '{`status`: {`version`: `1.0.0`, `code`: 210, `msg`: `Geocoder Results Address Not Identified.`, `total`: 0, `page`: 0, `pagesize`: 0}}';")
bad_geocoder = bad_geocoder_result.fetchall()
for item in bad_geocoder:
    rowid_to_delete.append(item[0])
    print(item)

# find records in database where results not returned
success_without_result = conn.execute("SELECT lab_address_data.ROWID FROM lab_address_data WHERE json_data = '{`status`: {`version`: `1.0.0`, `code`: 1, `msg`: `SuccessWithoutResult`, `total`: 0, `page`: 1, `pagesize`: 10}, `property`: []}';")
success_without = success_without_result.fetchall()
for item in success_without:
    rowid_to_delete.append(item[0])
    print(item)

invalid_parameter_result = conn.execute("SELECT lab_address_data.ROWID FROM lab_address_data WHERE json_data = '{`status`: {`version`: `1.0.0`, `code`: -4, `msg`: `Invalid Parameter Combination`, `total`: 0, `page`: 0, `pagesize`: 0}, `property`: []}';")
invalid_parameter = invalid_parameter_result.fetchall()
for item in invalid_parameter:
    rowid_to_delete.append(item[0])
    print(item)

# clean gps table
# {`results`: [], `status`: `ZERO_RESULTS`}
# {`error_message`: `You have exceeded your daily request quota for this API. We recommend registering for a key at the Google Developers Console: https://console.developers.google.com/apis/credentials?project=_`, `results`: [], `status`: `OVER_QUERY_LIMIT`}
gps_rowid_to_delete = []
zero_results_result = conn.execute("SELECT lab_address_GPS.ROWID FROM lab_address_GPS WHERE json_data = '{`results`: [], `status`: `ZERO_RESULTS`}';")
zero_results = zero_results_result.fetchall()
for item in zero_results:
    gps_rowid_to_delete.append(item[0])
    print(item)

over_limit_result = conn.execute("SELECT lab_address_GPS.ROWID FROM lab_address_GPS WHERE json_data = '{`error_message`: `You have exceeded your daily request quota for this API. We recommend registering for a key at the Google Developers Console: https://console.developers.google.com/apis/credentials?project=_`, `results`: [], `status`: `OVER_QUERY_LIMIT`}';")
over_limit = over_limit_result.fetchall()
for item in zero_results:
    gps_rowid_to_delete.append(item[0])
    print(item)

print(len(gps_rowid_to_delete))

# delete bad rows from GPS table
# acc2 = 0
# for rowid in gps_rowid_to_delete:
#     conn.execute("DELETE FROM lab_address_GPS WHERE ROWID='"+str(rowid)+"';")
#     conn.commit()
#     print("Row "+str(rowid)+" deleted. "+ str(acc2))
#     acc2+=1


# delete the bad rows
# acc = 0
# for rowid in rowid_to_delete:
#     conn.execute("DELETE FROM lab_address_data WHERE ROWID='"+str(rowid)+"';")
#     conn.commit()
#     print("Row "+str(rowid)+" deleted. "+ str(acc))
#     acc+=1


# find number of rows in lab_address_data (122 before deleting 48 bad rows)
lab_data_result = conn.execute("SELECT count(*) FROM lab_address_data;")
lab_data = lab_data_result.fetchall()
print(str(lab_data[0][0])+" rows in lab_address_data")

sample_json_result = conn.execute("SELECT json_data FROM lab_address_data WHERE address LIKE '%JACKSON, MICHIGAN';")
json_result = sample_json_result.fetchall()

# for item in json_result:
#     json_string_backtick = item[0]
#     json_string = json_string_backtick.replace("`","'")
#     #print(json_string)
#     sample_dict = ast.literal_eval(json_string)
#     # sample_dict = json.loads(json_string)
#     print(sample_dict['property'][0]['assessment']['calculations']['calcttlvalue'])
#     print(sample_dict['property'][0]['location']['latitude'])
#     print(sample_dict['property'][0]['location']['longitude'])

#{`status`: {`version`: `1.0.0`, `code`: -4, `msg`: `Invalid Parameter Combination`, `total`: 0, `page`: 0, `pagesize`: 0}, `property`: []}

conn.commit()
conn.close()


