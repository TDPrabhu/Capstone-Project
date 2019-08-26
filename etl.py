import pandas as pd
import os
import configparser
import json, boto3
import psycopg2
import configparser
import pyarrow as pa
import pyarrow.parquet as pq
from collections import defaultdict
from s3fs import S3FileSystem
from datetime import datetime
from datetime import datetime
from sql_queries import *

'''
Function which converts sas7bdat file format into parquet file format and loads into S3   
    * Maps the US states and checks for the null and mark them as other
    * Maps the country code and checks for the null and mark them as other
    * Converts the date
    * Changes the data type of a column 

'''
def upload_immigration_data_S3():
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    AWS_ACCESS_KEY_ID = config.get('default', 'AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = config.get('default', 'AWS_SECRET_ACCESS_KEY')
    file_path = config.get('default', 'INPUT_FILE_PATH')
    BUCKET_NAME = config.get('default', 'BUCKET_NAME')
    PREFIX = config.get('default', 'PREFIX')

    
    df = pd.read_sas(file_path, 'sas7bdat', encoding="ISO-8859-1")
    
    # Mapping the US states and if null exist or other than code mention into 'Other'

    d = defaultdict(lambda: 'Other')
    d.update({'MD':'MD','MA':'MA','AL':'AL','CA':'CA','NJ':'NJ','IL':'IL','AZ':'AZ','MO':'MO','NC':'NC','PA':'PA','KS':'KS','FL':'FL','TX':'TX','VA':'VA',
    'NV':'NV','CO':'CO','MI':'MI','CT':'CT','MN':'MN','UT':'UT','AR':'AR','TN':'TN','OK':'OK','WA':'WA','NY':'NY','GA':'GA','NE':'NE','KY':'KY',
    'SC':'SC','LA':'LA','NM':'NM','IA':'IA','RI':'RI','PR':'PR','DC':'DC','WI':'WI','OR':'OR','NH':'NH','ND':'ND','DE':'DE','OH':'OH','ID':'ID',
    'IN':'IN','AK':'AK','MS':'MS','HI':'HI','SD':'SD','ME':'ME','MT':'MT'})
    
    # Mapping the Country and if null exist or other than code mention into 'Other'
    d_country = defaultdict(lambda: 'Other')
    d_country.update(
    {
     582 :  'MEXICO Air Sea, and Not Reported (I-94, no land arrivals)',236 :  'AFGHANISTAN',101 :  'ALBANIA',316 :  'ALGERIA',102 :  'ANDORRA',324 :  'ANGOLA'
    ,529 :  'ANGUILLA',518 :  'ANTIGUA-BARBUDA',687 :  'ARGENTINA ',151 :  'ARMENIA',532 :  'ARUBA',438 :  'AUSTRALIA',103 :  'AUSTRIA',152 :  'AZERBAIJAN',512 :  'BAHAMAS',298 :  'BAHRAIN'
    ,274 :  'BANGLADESH',513 :  'BARBADOS',104 :  'BELGIUM',581 :  'BELIZE',386 :  'BENIN',509 :  'BERMUDA',153 :  'BELARUS',242 :  'BHUTAN',688 :  'BOLIVIA',717 :  'BONAIRE, ST EUSTATIUS, SABA' 
    ,164 :  'BOSNIA-HERZEGOVINA',336 :  'BOTSWANA',689 :  'BRAZIL',525 :  'BRITISH VIRGIN ISLANDS',217 :  'BRUNEI',105 :  'BULGARIA',393 :  'BURKINA FASO',243 :  'BURMA',375 :  'BURUNDI',310 :  'CAMEROON'
    ,326 :  'CAPE VERDE',526 :  'CAYMAN ISLANDS',383 :  'CENTRAL AFRICAN REPUBLIC',384 :  'CHAD',690 :  'CHILE',245 :  'CHINA, PRC',721 :  'CURACAO' ,270 :  'CHRISTMAS ISLAND',271 :  'COCOS ISLANDS'
    ,691 :  'COLOMBIA',317 :  'COMOROS',385 :  'CONGO',467 :  'COOK ISLANDS',575 :  'COSTA RICA',165 :  'CROATIA',584 :  'CUBA',218 :  'CYPRUS',140 :  'CZECH REPUBLIC',723 :  'FAROE ISLANDS (PART OF DENMARK)'
    ,108 :  'DENMARK',322 :  'DJIBOUTI',519 :  'DOMINICA',585 :  'DOMINICAN REPUBLIC',240 :  'EAST TIMOR',692 :  'ECUADOR',368 :  'EGYPT',576 :  'EL SALVADOR',399 :  'EQUATORIAL GUINEA'
    ,372 :  'ERITREA',109 :  'ESTONIA',369 :  'ETHIOPIA',604 :  'FALKLAND ISLANDS',413 :  'FIJI',110 :  'FINLAND',111 :  'FRANCE',601 :  'FRENCH GUIANA',411 :  'FRENCH POLYNESIA',387 :  'GABON'
    ,338 :  'GAMBIA',758 :  'GAZA STRIP' ,154 :  'GEORGIA',112 :  'GERMANY',339 :  'GHANA',143 :  'GIBRALTAR',113 :  'GREECE',520 :  'GRENADA',507 :  'GUADELOUPE',577 :  'GUATEMALA'
    ,382 :  'GUINEA',327 :  'GUINEA-BISSAU',603 :  'GUYANA',586 :  'HAITI',726 :  'HEARD AND MCDONALD IS.',149 :  'HOLY SEE/VATICAN',528 :  'HONDURAS',206 :  'HONG KONG',114 :  'HUNGARY'
    ,115 :  'ICELAND',213 :  'INDIA',759 :  'INDIAN OCEAN AREAS (FRENCH)' ,729 :  'INDIAN OCEAN TERRITORY' ,204 :  'INDONESIA',249 :  'IRAN',250 :  'IRAQ',116 :  'IRELAND',251 :  'ISRAEL'
    ,117 :  'ITALY',388 :  'IVORY COAST',514 :  'JAMAICA',209 :  'JAPAN',253 :  'JORDAN',201 :  'KAMPUCHEA',155 :  'KAZAKHSTAN',340 :  'KENYA',414 :  'KIRIBATI',732 :  'KOSOVO' ,272 :  'KUWAIT'
    ,156 :  'KYRGYZSTAN',203 :  'LAOS',118 :  'LATVIA',255 :  'LEBANON',335 :  'LESOTHO',370 :  'LIBERIA',381 :  'LIBYA',119 :  'LIECHTENSTEIN',120 :  'LITHUANIA',121 :  'LUXEMBOURG',214 :  'MACAU'
    ,167 :  'MACEDONIA',320 :  'MADAGASCAR',345 :  'MALAWI',273 :  'MALAYSIA',220 :  'MALDIVES',392 :  'MALI',145 :  'MALTA',472 :  'MARSHALL ISLANDS',511 :  'MARTINIQUE',389 :  'MAURITANIA'
    ,342 :  'MAURITIUS',760 :  'MAYOTTE (AFRICA - FRENCH)' ,473 :  'MICRONESIA, FED. STATES OF',157 :  'MOLDOVA',122 :  'MONACO',299 :  'MONGOLIA',735 :  'MONTENEGRO' ,521 :  'MONTSERRAT'
    ,332 :  'MOROCCO',329 :  'MOZAMBIQUE',371 :  'NAMIBIA',440 :  'NAURU',257 :  'NEPAL',123 :  'NETHERLANDS',508 :  'NETHERLANDS ANTILLES',409 :  'NEW CALEDONIA',464 :  'NEW ZEALAND'
    ,579 :  'NICARAGUA',390 :  'NIGER',343 :  'NIGERIA',470 :  'NIUE',275 :  'NORTH KOREA',124 :  'NORWAY',256 :  'OMAN',258 :  'PAKISTAN',474 :  'PALAU',743 :  'PALESTINE' ,504 :  'PANAMA'
    ,441 :  'PAPUA NEW GUINEA',693 :  'PARAGUAY',694 :  'PERU',260 :  'PHILIPPINES',416 :  'PITCAIRN ISLANDS',107 :  'POLAND',126 :  'PORTUGAL',297 :  'QATAR',748 :  'REPUBLIC OF SOUTH SUDAN'
    ,321 :  'REUNION',127 :  'ROMANIA',158 :  'RUSSIA',376 :  'RWANDA',128 :  'SAN MARINO',330 :  'SAO TOME AND PRINCIPE',261 :  'SAUDI ARABIA',391 :  'SENEGAL',142 :  'SERBIA AND MONTENEGRO'
    ,745 :  'SERBIA' ,347 :  'SEYCHELLES',348 :  'SIERRA LEONE',207 :  'SINGAPORE',141 :  'SLOVAKIA',166 :  'SLOVENIA',412 :  'SOLOMON ISLANDS',397 :  'SOMALIA',373 :  'SOUTH AFRICA',276 :  'SOUTH KOREA'
    ,129 :  'SPAIN',244 :  'SRI LANKA',346 :  'ST. HELENA',522 :  'ST. KITTS-NEVIS',523 :  'ST. LUCIA',502 :  'ST. PIERRE AND MIQUELON',524 :  'ST. VINCENT-GRENADINES',716 :  'SAINT BARTHELEMY' 
    ,736 :  'SAINT MARTIN' ,749 :  'SAINT MAARTEN' ,350 :  'SUDAN',602 :  'SURINAME',351 :  'SWAZILAND',130 :  'SWEDEN',131 :  'SWITZERLAND',262 :  'SYRIA',268 :  'TAIWAN',159 :  'TAJIKISTAN'
    ,353 :  'TANZANIA',263 :  'THAILAND',304 :  'TOGO',417 :  'TONGA',516 :  'TRINIDAD AND TOBAGO',323 :  'TUNISIA',264 :  'TURKEY',161 :  'TURKMENISTAN',527 :  'TURKS AND CAICOS ISLANDS',420 :  'TUVALU'
    ,352 :  'UGANDA',162 :  'UKRAINE',296 :  'UNITED ARAB EMIRATES',135 :  'UNITED KINGDOM',695 :  'URUGUAY',163 :  'UZBEKISTAN',410 :  'VANUATU',696 : 'VENEZUELA',266 : 'VIETNAM',469 : 'WALLIS AND FUTUNA ISLANDS'
    ,757 :  'WEST INDIES (FRENCH)' ,333 :  'WESTERN SAHARA',465 :  'WESTERN SAMOA',216 :  'YEMEN',139 :  'YUGOSLAVIA',301 :  'ZAIRE',344 :  'ZAMBIA',315 :  'ZIMBABWE'
    }
    )
    df['usa_states'] = df['i94addr'].map(d) #mapping the US states 
    df['country'] = df['i94res'].map(d_country) #mapping the country code  
    df['converted_date'] =  pd.to_timedelta(df.arrdate, unit="d") + pd.Timestamp(1960, 1, 1) # Converting the date 
    
    # changing the exisiting data type into required one. 
    df['string_date'] =df['converted_date'].astype(str)
    df['year'] = df['i94yr'].astype(int)
    df['month'] = df['i94mon'].astype(int)
    df['country_code'] = df['i94res'].astype(int)
    
    table_immigiration = pa.Table.from_pandas(df) #loading the data into parquet table from pandas data frame 
    file_path_source = 's3://capstone-project/immigration_test/source'
    file_path_result = 's3://capstone-project/immigration_test/result'
    source_key =  config.get('default', 'FILE_PATH_SOURCE')
    result_key =  config.get('default', 'FILE_PATH_RESULT')
    usademography_key =config.get('default', 'FILE_PATH_USADEMOGRAPHY')
    s3 = S3FileSystem(key=AWS_ACCESS_KEY_ID, secret=AWS_SECRET_ACCESS_KEY)
    pq.write_to_dataset(table=table_immigiration, root_path=source_key,filesystem=s3) #write the data into the S3 bucket (source) 
    pq.write_to_dataset(table=table_immigiration, root_path=result_key,filesystem=s3) #write the data into the S3 bucket (result)

'''
Function which copy the data from S3 to Redshift 

'''
    
def upload_usa_demography_s3():
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    AWS_ACCESS_KEY_ID     = config.get('default', 'AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = config.get('default', 'AWS_SECRET_ACCESS_KEY')
    file_path = config.get('default', 'FILE_PATH_SOURCE')
    BUCKET_NAME = config.get('default', 'BUCKET_NAME')
    FILE_PATH_USADEMOGRAPHY = config.get('default', 'FILE_PATH_USADEMOGRAPHY')
    session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(BUCKET_NAME)
    usademography_key =config.get('default', 'FILE_PATH_USADEMOGRAPHY')
    file_usa =config.get('default', 'file_usa')
    with open(file_usa, 'rb') as data:
        bucket.put_object(Key=FILE_PATH_USADEMOGRAPHY, Body=data)
        
'''
Function which copy the data from S3 to Redshift 

'''

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()   

'''
Function which inserts the data from Stagging table into the fact & dim table 

'''
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

'''
Function which list all the objects from the prefix provided and deletes the object . 

'''
def listfiles(client):
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    BUCKET_NAME = config.get('default', 'BUCKET_NAME')
    PREFIX = config.get('default', 'PREFIX')
    response = client.list_objects(Bucket=BUCKET_NAME, Prefix = PREFIX)
    for content in response.get('Contents', []):
        yield content.get('Key')
        

'''
Function validates the stgging and fact table count  . 

'''
def validation_records (cur, conn):
    cur.execute(validate_immigration_stagging_table)
    count_ims_table = cur.fetchone()
    immigration_stagging_count = count_ims_table[0]
    print (immigration_stagging_count)
    cur.execute(validate_fact_table)
    count_fact_table = cur.fetchone()
    fact_count = count_fact_table[0]
    print (fact_count)
    if immigration_stagging_count == fact_count:
        print ('Stagging and fact table count are equal')
    else:
        print ('Stagging and fact table count are not equal')
    
'''
main function which calls all the sub-routine 

'''
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    config.read('dl.cfg')
    AWS_ACCESS_KEY_ID     = config.get('default', 'AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = config.get('default', 'AWS_SECRET_ACCESS_KEY')
    BUCKET_NAME = config.get('default', 'BUCKET_NAME')
    PREFIX = config.get('default', 'PREFIX')
    client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3 = boto3.resource('s3',
         aws_access_key_id=AWS_ACCESS_KEY_ID,
         aws_secret_access_key= AWS_SECRET_ACCESS_KEY)
    cur = conn.cursor()
    
    listfiles(client)
    file_list = listfiles(client)
    for file in file_list:
        if file != PREFIX :
            obj = s3.Object(BUCKET_NAME, file)
            obj.delete()

    print ('executing function upload_immigration_data_S3 ...')    
    upload_immigration_data_S3()
    print ('executing function upload_usa_demography_s3 ....')   
    upload_usa_demography_s3()
    print ('executing function load_staging_tables ...')   
    load_staging_tables(cur, conn)
    print ('executing function insert_tables .....')   
    insert_tables(cur, conn)
    print ('executing function validation_records ......')   
    validation_records (cur, conn)
    print ('etl process completed successfuly......')  
    
    conn.close()


if __name__ == "__main__":
    main()