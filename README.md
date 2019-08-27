# Capstone-Project

**Objective :**  To Anlayse the total number of visa type of the immigrants and also to know the maximum count of visa type statewise.

The purpose of this project is to create an ETL pipeline(etl.py) which extracts the data (i94\_jan16\_sub.sas7bdat, usa-cities-demography.csv), convert them into parquet format and store them in the S3 bucket and then finally load into RedShift DB. From this DB the analyst can consume the data.

**Architecture Diagram :**

The dimenional model which i adopted is based on star schema.

For ETL(Extract , transform and load) i have used the python programming.For extraction and transformation i have used pandas and pyarrow libraries and to transfer the files to S3 bucket, i have used s3fs libraries. The script uses psycopg2 package for data munging and also to connect to the database and load the data. Before inserting the data, i have included the validation to avoid the redudant data.

 ![alt tag](https://github.com/TDPrabhu/Capstone-Project/blob/master/Architecture.PNG)
 
**Dataset :**

I have used the data set provided by the Udacity, of that i have choosen 2 different data set (i94\_jan16\_sub.sas7bdat , usa-cities-demography.csv ) .

   1)i94\_jan16\_sub.sas7bdat has more than 2.8 million records .
   2)usa-cities-demography.csv has nearly 3 thousand reocrds .

**Explore and Assess the Data :**
 
 Below is the assumption of the data columns, after analysis on the data fetched from i94\_jan16\_sub.sas7bdat.
 Loaded the complete data set into pandas and analysed the data set .

Example :- da.cicid.isnull().sum().sum() to check for the null values

   da.cicid.dtype  to check for the data type .

- CICID is a unique number for the immigrants . (no null values found ).
- I94res is country from where he has travlled . (no null values found ).
- i94addr is where the immigrants resides in USA .  (null values found).
- arrdate is date of arrrival .  (need to convert into timestamp )
- visatype is which type of visa he owns . (no null values found).

Used the defaultdict from collections pacakge to create a dictionary and map the correct values and replace with &#39;Other &#39; for null &amp; bad data .

Have created two defaultdict for the listed column(I94res, I94addr) and loaded them into the data frame (country ,usa\_states) respectively . This help to eliminate the null values and other bad data which are not listed in the dictionary. In future if we need to validate for the new state or country we can add the values in the dictionary an process the data .

Converted the arrdate to timestamp .

/\* df[&#39;converted\_date&#39;] =  pd.to\_timedelta(df.arrdate, unit=&quot;d&quot;) + pd.Timestamp(1960, 1, 1) \*/ .

The tools utilized on this project are the same as we have been learning during this Nanodegree.

-  Python
    - --Pyarrow
    - --Pandas
    - --Collections
    - --s3fs
- AWS S3
- AWS Redshift

**Data Model :**

![alt tag](https://github.com/TDPrabhu/Capstone-Project/blob/master/datamodel.PNG)

 
| immigration\_stagging | Stagging Table |
| --- | --- |
| usa\_demographic\_stagging | Stagging Table |
| dim\_visa | Dimension Table |
| dim\_date | Dimension Table |
| dim\_usa\_state | Dimension Table |
| dim\_country | Dimension Table |
| fact\_immigration | Fact Table |

| Table | Primary Key | Distribution-Style | Sort Key |
| --- | --- | --- | --- |
| dim\_visa | visa\_id | All | visa\_id |
| dim\_date | date\_id | All | date\_id |
| dim\_usa\_state | usa\_state\_id | All | usa\_state\_id |
| dim\_country | country\_id | All | country\_id |
| fact\_immigration | cicid | Key | cicid |

**Table Creation:**

Tables are created by executing the Python script create\_tables.py. The create\_tables.py in returns call the sql\_queries.py which has all the DDL statements in it.

**ETL Pipeline:**

 Python etl.py is used to extract &amp; transform the data from the file provided. It establishes the connection to the DB, then it extracts the required information from the files mentioned in the path and stores the data in to the appropriate tables. It checks for the duplicate before inserting the record into the tables. The code is modularized and provided all the comments.

 1)upload\_immigration\_data\_S3: Function will fetch immigration data and store it in the S3 bucket. All the transformation is done like checking for the data type, date format &amp; etc. defaultdict are used to map country and us states.

  -  The valid US states are loaded into the dictionary and checked for the same, if the column has other than values provided in dictionary it will be replaced it with Other.
- The valid Country and their respective country code are loaded into the dictionary and checked for the same, if the column has other than values provided in dictionary it will be replaced it with Other.

  Once all the transformation is done the data are loaded back into the pandas data frame, later it is converted into the parquet file. The parquet file in loaded into the S3 bucket.

  The parquet file is loaded into two folders Source &amp; Result.  Whenever the etl.py is run, the Files in the Result folder will be deleted, we delete the files in the folder because we load the **delta data** to redshift from this folder.

 2)upload\_usa\_demography\_S3: Function will fetch and load the usa-cities-demography.csv into S3 bucket.
 3)load\_staging\_tables : loads  the data from S3 bucket into the Redshift Stagging table .
 4)insert\_tables : inserts the data into the Redshift dim and fact table .
 5)validation\_records :-  Validate the record count of immigration\_stagging &amp; fact\_immigration table

If we are running this in the AWS cloud environment, create this etl.py as an lambda service and schedule it with the cloud watch service.

**Steps to execute the code :**

1. Open a new terminal
2. - !pip install pyarrow
   - !pip install s3fs

3. First execute the create\_tables.py.

   python create\_tables.py

4. Next Execute the etl.py

   python etl.py
   
   ![alt tag](https://github.com/TDPrabhu/Capstone-Project/blob/master/etl-pipeline.PNG)

 
Re run the create\_tables.py, whenever you do the change to sql\_queries.py or before you execute the etl.py

**Example query for song play analysis**

select state, visa\_type from
(
select state, visa\_type, rank () over ( partition by state order by agg\_tot desc ) agg\_values
from
(
select b.visa\_type,c.state,
count (1) agg\_tot
from public.fact\_immigration a,public.dim\_visa b ,public.dim\_usa\_state c
where a.visa\_id =b.visa\_id
and a.usa\_state\_id =c.usa\_state\_id
group by b.visa\_type,c.state
) )
where agg\_values = 1;


**Scenarios**

1. **The data was increased by 100x.** 
 
 -  In this project we have used Python for data processing, S3 for storage and Redshift as database. we can repalce                         python(etl.py) with Spark for data processing,. All the data processing stuffs can be handed over to Spark.  This make the data         processing still more faster. So the tech stack will be (Spark, S3 ,Redshift)
 - If we completely deploy our project in AWS environment, we can also try AWS Glue, it&#39;s an AWS managed service ETL tool. So, our      new tech stack will be (AWS GLUE, AWS S3, AWS Redshift / AWS Athena).

2. **The pipelines would be run daily by 7 am every day.**  

- We can use Airflow for orchestrating all the functions and schedule it on @daily 7 am
- If we are completely deploying our project in the AWS environment, then we can use the cloud watch event to schedule the job to run     daily on 7 am

3. **The database needed to be accessed by 100+ people.**

- Redshift is highly scalable, hence that will not be problem
- Can also use AWS Athena, as it is serverless query service. There is also no need to load S3 data into Athena, which makes it easier     and faster for consumer or analyst to gain insight. It also can access by 100+ people.
  Apache Spark, S3 (data partition based on year, month, day), Athena will make a good tech stack
