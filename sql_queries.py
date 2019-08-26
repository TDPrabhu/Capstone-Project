import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


""" 
 Create all the tables 
 
"""

stagging_immigration_table_create=  ("""Create table IF NOT EXISTS immigration_stagging
										(
											cicid      		 DOUBLE PRECISION ,
                                            i94yr            DOUBLE PRECISION ,
                                            i94mon           DOUBLE PRECISION ,
                                            i94cit           DOUBLE PRECISION ,
                                            i94res     		 DOUBLE PRECISION ,
                                            i94port    		 varchar(254),
                                            arrdate    		 DOUBLE PRECISION ,
                                            i94mode    		 DOUBLE PRECISION ,
                                            i94addr    		 varchar(254),
                                            depdate    		 DOUBLE PRECISION ,
                                            i94bir     		 DOUBLE PRECISION ,
                                            i94visa    		 DOUBLE PRECISION ,
                                            count      		 DOUBLE PRECISION ,
                                            dtadfile   		 varchar(254),
                                            visapost   		 varchar(254),
                                            occup      		 varchar(254),
                                            entdepa    		 varchar(254),
                                            entdepd    		 varchar(254),
                                            entdepu    		 varchar(254),
                                            matflag    		 varchar(254),
                                            biryear    		 DOUBLE PRECISION ,
                                            dtaddto    		 varchar(254),
                                            gender     		 varchar(1),
                                            insnum     		 varchar(254),
                                            airline    		 varchar(254),
                                            admnum      	 DOUBLE PRECISION ,
                                            fltno       	 varchar(254),
                                            visatype    	 varchar(254),
                                            usa_states  	 varchar(254),
                                            country			 varchar(254),
                                            converted_date	 timestamp,
                                            string_date      varchar(254),
                                            year             BIGINT,
                                            month            BIGINT,
                                            country_code     BIGINT
										)                               
									""")
stagging_usa_state_table_create = ("""CREATE TABLE IF NOT EXISTS usa_demographic_stagging(
                                    City                       varchar(254),
									State                      varchar(254),
									Median_Age                 DOUBLE PRECISION,
									Male_Population            DOUBLE PRECISION,
									Female_Population          DOUBLE PRECISION,
									Total_Population           int,
									Number_of_Veterans         DOUBLE PRECISION,
									Foreign_born               DOUBLE PRECISION,
									Average_Household_Size    DOUBLE PRECISION,
									State_Code                 varchar(254),
									Race                       varchar(254),
									Count                      int
                                    )
                                   """)

dim_visa_create = (""" create table IF NOT EXISTS dim_visa
						(	
							visa_id 			INT IDENTITY(1,1) sortkey,
							visa_type 			varchar(254),
							PRIMARY KEY (visa_id)							
						)diststyle all
                    """)

dim_date_create = (""" create table IF NOT EXISTS dim_date
							(	
								date_id 			INT IDENTITY(1,1) sortkey , 
								immigration_date 	varchar(254),
								month				DOUBLE PRECISION,
								year 				DOUBLE PRECISION ,
								PRIMARY KEY (date_id)
							)diststyle all
                    
                    """)

dim_usa_state_create = (""" create table IF NOT EXISTS dim_usa_state
							(
								usa_state_id 		INT IDENTITY(1,1) sortkey, 
								state_code  		varchar(254) ,
								state				varchar(254),
								PRIMARY KEY (usa_state_id)
							)diststyle all
                      """)

dim_country_create = ("""  create table IF NOT EXISTS dim_country 
						(
							country_id 			INT sortkey,
							country				varchar(254),
							PRIMARY KEY (country_id)
							
						)diststyle all
                     """)

fact_immigration_create = (""" create table IF NOT EXISTS fact_immigration
								( 
									cicid DOUBLE PRECISION sortkey, 
									country_id INT REFERENCES dim_country(country_id), 
									usa_state_id INT REFERENCES dim_usa_state(usa_state_id),
									date_id INT REFERENCES dim_date(date_id),  
									visa_id INT REFERENCES dim_visa(visa_id) distkey,
									PRIMARY KEY (cicid) 
								)
                        """)

# STAGING TABLES

""" 
 Load the data from S3 into staging tables  (Redshift)
 
"""

staging_immigration_copy = ("""  copy immigration_stagging from '{}'
                              credentials 'aws_iam_role={}' 
                              FORMAT AS PARQUET
                       """).format(config.get('S3','immmigration_data'),
                                    config.get('IAM_ROLE', 'ARN')
                                  )

staging_usa_demographic_copy = (""" copy usa_demographic_stagging from '{}'
									credentials 'aws_iam_role={}'
									CSV
							""").format(config.get('S3','demographic_data'), 
                                  config.get('IAM_ROLE', 'ARN')
                                 )

# FINAL TABLES

""" 
 Insert the data from Staging table into fact and dimnesion table  .
 
"""

dim_visa_table_insert = (
						"""
						    INSERT INTO dim_visa (visa_type) 
                            SELECT DISTINCT visatype
                            FROM immigration_stagging a
                            WHERE  a.visatype NOT IN (SELECT  c.visa_type FROM dim_visa c)
                        """
						)

dim_date_table_insert = (""" INSERT INTO dim_date (immigration_date,year, month)  
                          SELECT DISTINCT 
                                a.string_date,
                                a.year,
                                a.month
                            FROM immigration_stagging a
                            WHERE a.string_date NOT IN (SELECT  immigration_date FROM dim_date)
                            
						""")

dim_country_table_insert = (""" INSERT INTO dim_country (country_id,country)  
                            SELECT DISTINCT
                            	a.country_code,
                                a.country                         
                            FROM immigration_stagging a
                            WHERE a.country_code NOT IN (SELECT country_id FROM dim_country)
                            
							""")
					 
dim_usa_state_table_insert = (""" INSERT INTO dim_usa_state (state_code, state)  
									SELECT DISTINCT 
										a.state_code,
									    a.state
									FROM usa_demographic_stagging a
									WHERE a.state_code NOT IN (SELECT state_code FROM dim_usa_state)
                            
							  """)

dim_others_usa_state_table_insert = (""" INSERT INTO dim_usa_state (state_code, state)  
                                        values
                                        ('Other','Other')
							     """)

fact_immigration_table_insert = (""" INSERT INTO fact_immigration 
								 (
									cicid, 
									country_id,
									usa_state_id,
									date_id,
									visa_id 
								)
									SELECT DISTINCT 
										a.cicid,
									    d.country_id,
										e.usa_state_id,
										c.date_id,
										b.visa_id	
									FROM immigration_stagging a
										,dim_visa b
										,dim_date c
										,dim_country d
										,dim_usa_state e 
									WHERE a.visatype = b.visa_type 
									and   a.string_date = c.immigration_date
									and   a.country = d.country 
									and   a.usa_states = e.state_code 
									and a.cicid NOT IN (SELECT cicid FROM fact_immigration)
                            
							  """)

validate_immigration_stagging_table = (""" SELECT COUNT(*) FROM immigration_stagging """)
validate_fact_table     = (""" SELECT COUNT(*) FROM fact_immigration """)

# QUERY LISTS

create_table_queries = [stagging_immigration_table_create, stagging_usa_state_table_create,dim_visa_create,dim_date_create,dim_usa_state_create,dim_country_create,fact_immigration_create]
copy_table_queries   = [staging_immigration_copy, staging_usa_demographic_copy]
insert_table_queries = [ dim_visa_table_insert, dim_date_table_insert, dim_country_table_insert, dim_usa_state_table_insert,dim_others_usa_state_table_insert,fact_immigration_table_insert]