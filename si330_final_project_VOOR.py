import petl as etl
import sqlite3
import requests
import json
import time
import ast
import csv

#####################################
# John Voorhess SI330 Final Project #
#####################################
ONBOARD_API_KEY = "7a293014fc9cf968c31c53eb46916385"
HEADERS = {'apikey': ONBOARD_API_KEY, 'accept': 'application/json'}
BASE_URL = "https://search.onboard-apis.com/propertyapi/v1.0.0/saleshistory/detail?address1="
BASE_URL_ASSESSMENT = "https://search.onboard-apis.com/propertyapi/v1.0.0/assessment/detail?address1="
GOOGLE_MAPS_API_KEY = "AIzaSyDBwwT00ktZRGAnwc2lNKwJRxVMvVdo4IE"

#______________________________________________________
# Reference values for metrics:
AVERAGE_MI_HOME_PRICE = 135700 #source: https://www.zillow.com/mi/home-values/
TOP_TEN_PRICE_LIST = [185700,93700,206800,92900,174500,305800,157300,113500,359000,80300] #source: https://www.zillow.com/mi/home-values/
MOST_FREQUENT_PRICE_LIST = [94400,123200,160500,74000,118000] #source: https://www.zillow.com/mi/home-values/
LEAST_FREQUENT_PRICE_LIST = [159400,75300,76200,58400,137700] #source: https://www.zillow.com/mi/home-values/
AVERAGE_MOST_FREQUENT_PRICE = sum(MOST_FREQUENT_PRICE_LIST) / float(len(MOST_FREQUENT_PRICE_LIST))
AVERAGE_LEAST_FREQUENT_PRICE = sum(LEAST_FREQUENT_PRICE_LIST) / float(len(LEAST_FREQUENT_PRICE_LIST))
AVERAGE_TOP_TEN_PRICE = sum(TOP_TEN_PRICE_LIST) / float(len(TOP_TEN_PRICE_LIST))

################
# Heat map link:
# http://bit.ly/2ySN07V
# Point map link (with addresses)
# http://bit.ly/2BPs9Bc
#######################

# Open a connection to the database
conn = sqlite3.connect('drug_labs.db')

# # create a petl object for the contents of the csv file
# mi_labs = etl.fromcsv('mi_drug_labs.csv')

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

# create_drug_lab_full = """
# CREATE TABLE IF NOT EXISTS drug_lab_full (
# address varchar,
# city varchar,
# state varchar,
# zip varchar,
# calculated_value int,
# latitude float,
# longitude float,
# sq_ft int,
# baths float,
# beds float
# )
# """
# conn.execute(create_drug_lab_full)

# # run create statement to create lab table
# conn.execute(create_lab)
# conn.execute(create_lab_address_data)

# # populate lab table from previously instantiated petl object
# mi_labs.todb(conn,'lab')

# check number of records in table
count = conn.execute("SELECT count(*) FROM lab;")
print("lab table - "+str(count.fetchone()[0]))

count = conn.execute("SELECT count(*) FROM lab_address_GPS;")
print("lab_address_GPS - "+str(count.fetchone()[0]))

# query all addresses and json_data form lab_address_GPS
# parse json data to extract latitude and longitude
#
json_gps_result = conn.execute("SELECT address,city, state, json_data FROM lab_address_GPS;")
json_gps = json_gps_result.fetchall()
with open('gps.csv', 'w', newline='') as csvfile:
    for item in json_gps:
        json_string_backtick = item[3]
        json_string = json_string_backtick.replace("`","'")
        sample_dict = ast.literal_eval(json_string)
        address = item[0]
        city = item[1]
        state = item[2]
        full_address = address+" "+city+" "+state
        latitude = str(sample_dict['results'][0]['geometry']['location']['lat'])
        longitude = str(sample_dict['results'][0]['geometry']['location']['lng'])
        gpswriter = csv.writer(csvfile)
        gpswriter.writerow([full_address,latitude,longitude])

# Write drug_lab_full data to csv for more visualizations:
# drug_lab_full_csv_result = conn.execute("SELECT * from drug_lab_full;")
# drug_lab_full_csv = drug_lab_full_csv_result.fetchall()
# with open('druglabs.csv', 'w', newline='') as csvfile:
#     for item in drug_lab_full_csv:
#         print(item)
#         druglabwriter = csv.writer(csvfile)
#         druglabwriter.writerow(item)

# query google API for GPS coords and insert them into GPS coord table
address = conn.execute("SELECT address, city, state FROM lab;")
addresses = address.fetchall()
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
# API calls to ONBOARD API broken into groups over several days
city_count_result = conn.execute("SELECT city, count(*) from lab GROUP BY city ORDER BY count(*) DESC;")
city_count = city_count_result.fetchall()
# print(city_count)

# query Battle Creek addresses:
battle_creek_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city = 'BATTLE CREEK';")
battle_creek_addresses = battle_creek_address_result.fetchall()
battle_creek_address_tuple_list = []
for address in battle_creek_addresses:
    address_1 = address[0]
    address_2 = address[1]+", "+address[2]
    battle_creek_address_tuple_list.append((address_1,address_2))

# query Port Huron addresses:
port_huron_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city = 'PORT HURON';")
port_huron_addresses = port_huron_address_result.fetchall()
port_huron_address_tuple_list = []
for address in port_huron_addresses:
    address_1 = address[0]
    address_2 = address[1]+", "+address[2]
    port_huron_address_tuple_list.append((address_1,address_2))

# query Jackson addresses
jackson_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city = 'JACKSON';")
jackson_addresses = jackson_address_result.fetchall()
jackson_address_tuple_list = []
for address in jackson_addresses:
    address_1 = address[0]
    address_2 = address[1]+", "+address[2]
    jackson_address_tuple_list.append((address_1,address_2))

# query Allegan addresses
allegan_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city = 'ALLEGAN';")
allegan_addresses = allegan_address_result.fetchall()
allegan_address_tuple_list = []
for address in allegan_addresses:
    address_1 = address[0]
    address_2 = address[1]+", "+address[2]
    allegan_address_tuple_list.append((address_1,address_2))

# query Kalamazoo addresses
kalamazoo_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city = 'KALAMAZOO';")
kalamazoo_addresses = kalamazoo_address_result.fetchall()
kalamazoo_address_tuple_list = []
for address in kalamazoo_addresses:
    address_1 = address[0]
    address_2 = address[1]+", "+address[2]
    kalamazoo_address_tuple_list.append((address_1,address_2))

# on Thusday, I used the sorted list of cities to assemble a list of cities near the 500 calls per day limit
thursday_address_tuple_list = []
thursday_city_list = ['PORTAGE','THREE RIVERS','FLINT','STURGIS','PAW PAW','COLDWATER','OTSEGO','PLAINWELL','LANSING','OWOSSO','VICKSBURG','DOWAGIAC','GALESBURG','GOBLES','HARTFORD','CHARLOTTE','DECATUR','HASTINGS','LAWTON','NILES','DELTON','WHITE PIGEON','HOLLAND','BENTON HARBOR','CONSTANTINE','BRONSON','MARCELLUS','MATTAWAN','CASSOPOLIS','COLOMA','EDWARDSBURG','HILLSDALE']
for thursday_city in thursday_city_list:
    thursday_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city = '"+thursday_city+"';")
    thursday_addresses = thursday_address_result.fetchall()
    for address in thursday_addresses:
        address_1 = address[0]
        address_2 = address[1]+", "+address[2]
        thursday_address_tuple_list.append((address_1,address_2))

# on Friday, I added the previously called cities to thursday's list to assemble a list of cities to call
list_to_append = ['KALAMAZOO','ALLEGAN','JACKSON','PORT HURON','BATTLE CREEK']
already_in = list_to_append + thursday_city_list
friday_address_tuple_list = []
friday_address_result = conn.execute("SELECT address, city, state FROM lab WHERE city NOT IN ('KALAMAZOO', 'ALLEGAN', 'JACKSON', 'PORT HURON', 'BATTLE CREEK', 'PORTAGE', 'THREE RIVERS', 'FLINT', 'STURGIS', 'PAW PAW', 'COLDWATER', 'OTSEGO', 'PLAINWELL', 'LANSING', 'OWOSSO', 'VICKSBURG', 'DOWAGIAC', 'GALESBURG', 'GOBLES', 'HARTFORD', 'CHARLOTTE', 'DECATUR', 'HASTINGS', 'LAWTON', 'NILES', 'DELTON', 'WHITE PIGEON', 'HOLLAND', 'BENTON HARBOR', 'CONSTANTINE', 'BRONSON', 'MARCELLUS', 'MATTAWAN', 'CASSOPOLIS', 'COLOMA', 'EDWARDSBURG', 'HILLSDALE', 'SOUTH BOARDMAN', 'SOUTH GULL L', 'SPARTA', 'STERLING HEIGHTS', 'TAWAS CITY', 'TUSTIN', 'WARREN', 'WEST OLIVE', 'WILSON', 'WOLVERINE', 'WYOMING', 'YPSILANTI');")
friday_addresses = friday_address_result.fetchall()
for address in friday_addresses:
    address_1 = address[0]
    address_2 = address[1]+", "+address[2]
    friday_address_tuple_list.append((address_1,address_2))

# query api for all friday addresses:
# for address_tuple in friday_address_tuple_list:
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
#
#     conn.execute("INSERT INTO lab_address_data (address, json_data) VALUES ('"+ coupled_address_tuple +"', '"+ json_string +"');")


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
#
#     conn.execute("INSERT INTO lab_address_data (address, json_data) VALUES ('"+ coupled_address_tuple +"', '"+ json_string +"');")


# find records in lab_address table where address was not found
rowid_to_delete = []
bad_geocoder_result = conn.execute("SELECT lab_address_data.ROWID FROM lab_address_data WHERE json_data = '{`status`: {`version`: `1.0.0`, `code`: 210, `msg`: `Geocoder Results Address Not Identified.`, `total`: 0, `page`: 0, `pagesize`: 0}}';")
bad_geocoder = bad_geocoder_result.fetchall()
for item in bad_geocoder:
    rowid_to_delete.append(item[0])
    print(item)
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
multiple_properties_result = conn.execute("SELECT lab_address_data.ROWID FROM lab_address_data WHERE json_data = '{`status`: {`version`: `1.0.0`, `code`: 209, `msg`: `Geocoder Multiple Properties Error.`, `total`: 0, `page`: 0, `pagesize`: 0}}';")
multiple_properties = multiple_properties_result.fetchall()
for item in multiple_properties:
    rowid_to_delete.append(item[0])
    print(item)
invalid_parameter_request_result = conn.execute("SELECT lab_address_data.ROWID FROM lab_address_data WHERE json_data = '{`status`: {`version`: `1.0.0`, `code`: -5, `msg`: `Invalid Parameter in Request`, `total`: 0, `page`: 0, `pagesize`: 0}}';")
invalid_parameter_request = invalid_parameter_request_result.fetchall()
for item in invalid_parameter_request:
    rowid_to_delete.append(item[0])
    print(item)

# find records in gps table where address was not found
gps_rowid_to_delete = []
zero_results_result = conn.execute("SELECT lab_address_GPS.ROWID FROM lab_address_GPS WHERE json_data = '{`results`: [], `status`: `ZERO_RESULTS`}';")
zero_results = zero_results_result.fetchall()
for item in zero_results:
    gps_rowid_to_delete.append(item[0])
    print(item)
over_limit_result = conn.execute("SELECT lab_address_GPS.ROWID FROM lab_address_GPS WHERE json_data = '{`error_message`: `You have exceeded your daily request quota for this API. We recommend registering for a key at the Google Developers Console: https://console.developers.google.com/apis/credentials?project=_`, `results`: [], `status`: `OVER_QUERY_LIMIT`}';")
over_limit = over_limit_result.fetchall()
for item in over_limit:
    gps_rowid_to_delete.append(item[0])
    print(item)

# delete bad rows from GPS table
# acc2 = 0
# for rowid in gps_rowid_to_delete:
#     conn.execute("DELETE FROM lab_address_GPS WHERE ROWID='"+str(rowid)+"';")
#     conn.commit()
#     print("Row "+str(rowid)+" deleted. "+ str(acc2))
#     acc2+=1

# delete the bad rows from lab_address_data
# acc = 0
# for rowid in rowid_to_delete:
#     conn.execute("DELETE FROM lab_address_data WHERE ROWID='"+str(rowid)+"';")
#     conn.commit()
#     print("Row "+str(rowid)+" deleted. "+ str(acc))
#     acc+=1

# find number of rows in lab_address_data
lab_data_result = conn.execute("SELECT count(*) FROM lab_address_data;")
lab_data = lab_data_result.fetchall()
print("lab_address_data - "+ str(lab_data[0][0]))

# query all rows from lab_address_data, parse data, insert into drug_lab_full table
sample_json_result = conn.execute("SELECT address,json_data FROM lab_address_data;")
json_result = sample_json_result.fetchall()
for item in json_result:
    json_string_backtick = item[1]
    json_string = json_string_backtick.replace("`","'")
    sample_dict = ast.literal_eval(json_string)
    full_address = item[0]
    address_list = full_address.split(',')
    address = address_list[0]
    city = address_list[1]
    state = address_list[2]
    zip = sample_dict['property'][0]['address']['postal1']
    calculated_value_string = sample_dict['property'][0]['assessment']['calculations']['calcttlvalue']
    latitude_string = sample_dict['property'][0]['location']['latitude']
    longitude_string = sample_dict['property'][0]['location']['longitude']
    sq_ft_string = sample_dict['property'][0]['building']['size']['universalsize']
    baths_string = sample_dict['property'][0]['building']['rooms']['bathstotal']
    beds_string = sample_dict['property'][0]['building']['rooms']['beds']
    calculated_value = int(calculated_value_string)
    latitude = float(latitude_string)
    longitude = float(longitude_string)
    sq_ft = int(sq_ft_string)
    baths = float(baths_string)
    beds = float(beds_string)
    #conn.execute("INSERT INTO drug_lab_full (address, city, state, zip, calculated_value, latitude, longitude, sq_ft, baths, beds) VALUES ('"+address+"', '"+city+"', '"+state+"', '"+zip+"', '"+str(calculated_value)+"', '"+str(latitude)+"', '"+str(longitude)+"', '"+str(sq_ft)+"', '"+str(baths)+"', '"+str(beds)+"');")

drug_lab_full_result = conn.execute("SELECT count(*) from drug_lab_full;")
drug_lab_count = drug_lab_full_result.fetchall()
print("drug_lab_full table - "+ str(drug_lab_count[0][0]))

# find top ten zip codes with highest average value, excluding trailer park value
average_value_result = conn.execute("SELECT zip, avg(calculated_value) as av_value FROM drug_lab_full WHERE calculated_value != 0 AND calculated_value != 1288300 GROUP BY zip ORDER BY av_value DESC LIMIT 10;")
average_value = average_value_result.fetchall()
print(" ")
print("Top ten zip codes with highest average value:")
i = 0
for item in average_value:

    print(str(item[0]+" - "+str(format(item[1],'.2f')))+" which is "+format((item[1]/TOP_TEN_PRICE_LIST[i])*100,'.2f')+"% of average.")
    i+=1

# find top 5 most frequent cities
frequency_result = conn.execute("SELECT city, count(ROWID) as number FROM drug_lab_full GROUP BY city ORDER BY number DESC LIMIT 5;")
frequency = frequency_result.fetchall()
print(" ")
print("Top five most frequently appearing cities are:")
for item in frequency:
    print(item)

# find average value for top 5 most frequent cities
top5_av_result = conn.execute("SELECT avg(calculated_value) FROM drug_lab_full WHERE calculated_value != 0 AND ROWID IN (SELECT count(ROWID) as number FROM drug_lab_full GROUP BY city ORDER BY number DESC LIMIT 5);")
top5_av = top5_av_result.fetchall()
print("Average calculated value for five most frequent cities - "+format(top5_av[0][0],'.2f'))
print("which is "+format((top5_av[0][0]/AVERAGE_MOST_FREQUENT_PRICE)*100,'.2f')+"% of the average.")

# find bottom 5 most frequent cities
bottom_frequency_result = conn.execute("SELECT city, count(ROWID) as number FROM drug_lab_full GROUP BY city ORDER BY number ASC LIMIT 5;")
bottom_frequency = bottom_frequency_result.fetchall()
print(" ")
print("Five least frequently appearing cities are:")
for item in bottom_frequency:
    print(item)

# find average value for top 5 most frequent cities
bottom5_av_result = conn.execute("SELECT avg(calculated_value) FROM drug_lab_full WHERE calculated_value != 0 AND ROWID IN (SELECT count(ROWID) as number FROM drug_lab_full GROUP BY city ORDER BY number ASC LIMIT 5);")
bottom5_av = bottom5_av_result.fetchall()
print("Average calculated value for five least frequent cities - "+format(bottom5_av[0][0],'.2f'))
print("which is "+format((bottom5_av[0][0]/AVERAGE_LEAST_FREQUENT_PRICE)*100,'.2f')+"% of the average.")

average_bathroom_result = conn.execute("SELECT avg(baths) FROM drug_lab_full WHERE baths !=0;")
average_bathroom = average_bathroom_result.fetchall()
print(" ")
print("Average number of bathrooms - "+format(average_bathroom[0][0],'.2f'))

average_sq_ft_result = conn.execute("SELECT avg(sq_ft) FROM drug_lab_full WHERE sq_ft !=0;")
average_sq_ft = average_sq_ft_result.fetchall()
print(" ")
print("Average square footage - "+format(average_sq_ft[0][0],'.2f'))

average_beds_result = conn.execute("SELECT avg(beds) FROM drug_lab_full WHERE beds !=0;")
average_beds = average_beds_result.fetchall()
print(" ")
print("Average number of bedrooms - "+format(average_beds[0][0],'.2f'))

# find average value for all drug labs minus the trailer park value
average_michigan_value_result = conn.execute("SELECT avg(calculated_value) from drug_lab_full WHERE calculated_value != 0 AND calculated_value != 1288300;")
average_michigan_value = average_michigan_value_result.fetchall()
print(" ")
print("Average calculated value for all Michigan drug labs - "+format(average_michigan_value[0][0],'.2f'))
print("which is "+format((average_michigan_value[0][0]/AVERAGE_MI_HOME_PRICE)*100,'.2f')+"% of the average price of a home in Michigan.")


conn.commit()
conn.close()


