# Databricks notebook source
# MAGIC %md 
# MAGIC ##Leave Encashment Process
# MAGIC

# COMMAND ----------

# DBTITLE 1,Function-write_parquet_data
'''
write file in parquet format from dataframe
     Author: Manoj Kumar
     Version: Initial
     Version Date: Aug 29, 2023
     Passing Parameter:  dataset, targetname, writemode)
     Return: dataset count

'''
def write_parquet_data( dataset, targetname, writemode):
    #dataset is a dataframe that has to be written
    #targetname defines target location and file name
    #writemode defines mode in wihich dataset will be written to target location
    
    if(writemode == 'overwrite'):
        dataset.repartition(1).write.mode(writemode).parquet(targetname)  
    else:
        dataset.write.mode(writemode).parquet(targetname)  
    return dataset.count()

# COMMAND ----------

# DBTITLE 1,Function-get_delimited_data
'''
Read csv file from data lake
     Author: Manoj Kumar
     Version: Initial
     Version Date: Aug 29, 2023
     Passing Parameter:  dataformat, seperator, cuttime, mntpt
     Return: dataset 

'''

def get_delimited_data( dataformat, seperator, mntpt):
    #dataformat define file format like csv
    #seperator define field seperator like (comma) as , (pipe) as |
    #cuttime define files modified before
    #mntpt defines mountpint to be used for reading files with file name

    try: 
        rawdf = spark.read.format(dataformat).options(
                header="true", inferSchema="true", sep = seperator ).load(mntpt)
    except:    
        rawdf = spark.sparkContext.emptyRDD()
    return rawdf

# COMMAND ----------

# DBTITLE 1,Function-write_sql
'''
Process to write data into sql table
     Author: Manoj Kumar Bhagavathi
     Version: Initial
     Version Date: Aug 29, 2023
     Passing Parameter:  df,     table_name
     Return: dataset count

'''

def write_sql( df,table_name,writemode):

    url = "jdbc:sqlserver://xyzserver.database.windows.net:1433;database=employee"
    properties = {
        "user": "dbadmin",
        "password": "data@123",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    # Write data to SQL Server table
    df.write.jdbc(url=url, table=table_name, mode=writemode, properties=properties)

    return df.count()

# COMMAND ----------

# DBTITLE 1,Function - encashment_process
'''
Process to generate leave encashment details
     Author: Manoj Kumar
     Version: Initial
     Version Date: Aug 29, 2023
     Passing Parameter:       
     Return: dataset

'''
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType,  StringType,DateType

def encashment_process():

    fileformat = "csv"
    separator = "|"
    current_date = spark.sql("SELECT current_date()").collect()[0][0]
    # Add five days to the current date
    future_date = spark.sql(f"SELECT date_add('{current_date}', 5)").collect()[0][0]
    future_date_str = future_date.strftime('%Y-%m-%dT%H:%M:%S')
    targetname_silver ="/mnt/silver/parquet/employee_details"
    inputfilename = "/mnt/input_files"
    targetname_gold = "/mnt/gold/parquet/employee_details"
    written_mode_append = "append"
    written_mode_overwrite = "overwrite"
    try:
        #read data from bronze
        inputdf = get_delimited_data(fileformat, separator, inputfilename) #read file

        #write as parquet file in silver employee_encashment_details
        writecount = write_parquet_data( inputdf,  targetname_silver ,"overwrite")

        # read data from parquet file in silver zone
        encash_df = spark.read.option('header','true').option("inferSchema","true").parquet(targetname_silver)

        #find the difference between current date and joining date
        encash_df = encash_df.withColumn("year_diff", f.round(f.months_between(f.current_date(),f.col('HIRE_DATE'))/f.lit(12),2))

        # ENCASHMENT_AMT column  calculation
        encash_df = encash_df.withColumn('ENCASHMENT_AMT', f.when((f.col('year_diff') > f.lit(5)) & (f.col('LEAVE_BALANCE') >= f.lit(30)) , 30 *8 * f.col('HOURLY_RATE')  ).otherwise(f.lit(0)))

        # LEAVE_BALANCE column  calculation
        encash_df = encash_df.withColumn('LEAVE_BALANCE', f.when((f.col('year_diff') > f.lit(5)) & (f.col('LEAVE_BALANCE') >= f.lit(30)) , f.col('LEAVE_BALANCE')-f.lit(25)   ).otherwise(f.col('LEAVE_BALANCE')))

        # Adding payment PAYMENT_DATE
        encash_df = encash_df.withColumn('PAYMENT_DATE',f.lit(f.current_date()))

        #Select only eligible encahment employee details
        final_df = encash_df.select('EMPLOYEE_ID','LEAVE_BALANCE','PAYMENT_DATE','ENCASHMENT_AMT','HOURLY_RATE') \
            .filter(encash_df.ENCASHMENT_AMT > 0 )
            
        final_df = final_df.withColumn('TOTAL_NUMBER_OF_DAYS', f.lit(30)).drop('LEAVE_BALANCE')
        encash_df = encash_df.drop('year_diff','ENCASHMENT_AMT')

        # Select employee details columns
        employee_df = encash_df.select('EMPLOYEE_ID','FIRST_NAME','LAST_NAME','PHONE_NUMBER','LEAVE_BALANCE','HOURLY_RATE','HIRE_DATE') \

        #write as parquet file in gold employee_encashment_details
        writecount = write_parquet_data( final_df,  targetname_gold ,"overwrite")

        #Add data into sql server for employee_details
        sql_writecount = write_sql(employee_df,'employee_details',written_mode_overwrite)

        #Append  data into sql server for employee_details
        sql_writecount = write_sql(final_df,'employee_encashment_details',written_mode_append)

    except Exception as e :
        print("Error :",e)
    return encash_df



# COMMAND ----------


ret = encashment_process()
display(ret)


# COMMAND ----------

# path_to_data = '/mnt/gold/parquet/employee_details/'
# employee_details = spark.read.parquet(path_to_data)
# display(employee_details)
# employee_details.show()
