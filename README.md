# Capstone-Project

**Objective :** 
 The purpose of this project is to create an ETL pipeline (etl.py) which extracts the data (i94_jan16_sub.sas7bdat, usa-cities-demography.csv), converts it to a parquet format and store them in the S3 bucket. From there the data is finally loaded into RedShift DB where the analysts can consume the data and perform any statistical analysis or exploratory data analysis.

**Architecture Diagram :**

The dimenional model which i adopted is based on star schema.

For extraction and transformation I have used pandas and pyarrow libraries and to transfer the files to amazon S3 bucket, I have used s3fs libraries. The script uses psycopg2 package for data munging. Before inserting any data, I have performed the validations in order to avoid any redundancy.

 ![alt tag](https://github.com/TDPrabhu/Capstone-Project/blob/master/Architecture.PNG)
 
**Dataset :**

I have used the data set provided by the Udacity, of that i chose 2 different data set (i94\_jan16\_sub.sas7bdat , usa-cities-demography.csv ) .

   1)i94\_jan16\_sub.sas7bdat has more than 2.8 million records .
   2)usa-cities-demography.csv has nearly 3 thousand records .

**Explore and Assess the Data :**
  
Based on analyzing the data from 'i94_jan16_sub.sas7bdat' below are some of the assumptions for the required data columns:

Example :- da.cicid.isnull().sum().sum() to check for the null values

           da.cicid.dtype  to check for the data type .

- CICID is a unique number for the immigrants. (No null values found).
- I94res is country from where one has travelled. (No null values found).
- I94addr is where the immigrants resides in USA . (Found null values)..
- arrdate is date of arrrival . (Convert it to timestamp format).
- visatype is the type of visa which one owns . (No null values found).

I have created two defaultdict for the columns (I94res and I94addr, loaded them into a data frame (country ,usa_states) respectively . This will help us to eliminate any null values or bad data which are not listed in the dictionary. In future, if we need to validate for the new state or country we can add the values in the dictionary and process the data.

Converted the arrdate to timestamp .

/\* df[&#39;converted\_date&#39;] =  pd.to\_timedelta(df.arrdate, unit=&quot;d&quot;) + pd.Timestamp(1960, 1, 1) \*/ .

The tools utilized on this project are the same as we have been learning during the course of the Nanodegree.
-  Python
    - --Pyarrow
    - --Pandas
    - --Collections
    - --s3fs
- AWS S3
- AWS Redshift

**Data Model :**

![alt tag](https://github.com/TDPrabhu/Capstone-Project/blob/master/datamodel.PNG)

 

| immigration\_stagging | Staging Table |
| --- | --- |
| usa\_demographic\_stagging | Staging Table |
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

Tables are created by executing the script create_tables.py. The scripts will call the sql_queries.py which has all the required DDL statements in it.

**ETL Pipeline:**

etl.py is used to extract and transform the data from the file. It establishes a connection to the database; it extracts the required information from the files mentioned in the path and stores the data in to the appropriate staging tables. It checks for the duplicate before inserting the record into the facts and dim. The code is modularized and comments are included explaining their function.

 1)upload\_immigration\_data\_S3:This function will fetch the immigration data and store it in the S3 bucket. For all the transformation like checking the data type, date format, the defaultdict is used to map the country and State for US.

  - The valid US states are loaded into the dictionary, if the column doesn’t have the values provided, it will be replaced with             "Other".
  
 - The valid Country and their respective country code are loaded into the dictionary , if the column doesnt have the values provided in    dictionary it will be replaced with "Other".

 - Once all the required transformation is done, the data is then loaded to a pandas data frame, converted to parquet file format and      then moved into the S3 bucket.

 - The parquet file is loaded into two folders Source & Result. The source will contain all the files and the result folder will only      have the  **delta files** .

 2)upload\_usa\_demography\_S3: Function will fetch and load the usa-cities-demography.csv into S3 bucket.
 
 3)load\_staging\_tables : Loads the data from S3 bucket into the Redshift Staging table.
 
 4)insert\_tables : Inserts the data into the dim and fact table.
 
 5)validation\_records :- Validate the record count of immigration\_stagging &amp; fact\_immigration table

If we are running this in the AWS cloud environment, create this etl.py as an lambda service and schedule it with the cloud watch service.

**Steps to execute the code :**

1. Open a new terminal
2. Install the below listed libraries 
   - !pip install pyarrow
   - !pip install s3fs

3. First execute the create\_tables.py.

   python create\_tables.py

4. Next Execute the etl.py

   python etl.py
   
   ![alt tag](https://github.com/TDPrabhu/Capstone-Project/blob/master/etl-pipeline.PNG)

 
Re run the create\_tables.py, whenever you do the change to sql\_queries.py or before you execute the etl.py

**Example query for analysis**

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
 
 - In this project we have used Python for data processing, S3 for storage and Redshift as database. We can replace the python (etl.py)    with Spark. All the data processing can be handed over to Spark which makes the process robust and very fast. So the technology stack    will be 
       1) Spark for data processing 
       2) S3 for file storage 
       3) Redshift as a database
       
 - If we deploy our project in AWS environment, we can also try with AWS Glue, it&#39;s. If So, then the new tech stack will be (AWS        GLUE, AWS S3, AWS Redshift / AWS Athena).

2. **The pipelines would run daily by 7 am every day.**  

- We can use Airflow for orchestrating all the tasks and schedule it @daily 7 am.
- If we are running this in the AWS cloud environment, create this etl.py as a lambda service and schedule it with the cloud watch event.

  [https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/RunLambdaSchedule.html]
  [https://medium.com/blogfoster-engineering/running-cron-jobs-on-aws-lambda-with-scheduled-events-e8fe38686e20]
  

3. **The database needed to be accessed by 100+ people.**

- Redshift is highly scalable, hence this will not be problem.
- We can also use AWS Athena, as it is a  serverless query service. One advantage would be that we would not load the S3 data into Athena. This makes the entire process to be fast and efficient for data consumer to gain insight
     1) Apache Spark
     2) S3 (data partition based on year, month, day) 
     3) Athena .
