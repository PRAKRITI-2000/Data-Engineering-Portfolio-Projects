### Setting up the environment
from pyspark.sql import functions as f
from pyspark.sql.types import*
from pyspark import pipelines as dp 
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("MediCore Health").getOrCreate()

### Building the ingestion code with the following requiements specified by the client.
@dp.table(name="resillient_bronze_ingestion",comment="Here we are ingesting our data based on the requirements specified in the above mentioned pointers")

def resillient_bronze_ingestion():
    df=spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemaLocation","/Volumes/medicore_health_catalog/staging/raw_data_schema_location").option("cloudFiles.inferColumnTypes","false").option("header","true").option("cloudFiles.schemaEvolutionMode","addNewColumns").option("cloudFiles.rescuedDataColumn","_rescuedDataColumn").load("/Volumes/medicore_health_catalog/staging/raw_data/").withColumn("ingestion_timestamp",f.current_timestamp()).withColumn("file_name",f.input_file_name())
    
    df= df.withColumn("HospitalID",f.col("HospitalID").try_cast("integer"))
    df=df.withColumn("LastVisitDate",f.col("LastVisitDate").try_cast("date"))
    return df

# Retrieve the secret from Databricks Secret Scope and raising an error if the salt key is missing
salt_key=spark.conf.get("pii.salt_key")
if not salt_key:
    raise ValueError("SSN salt key is missing.")    

### Defining the leak-protection function

def leak_protection(df):
    req_columns=[col for col in df.columns if "ssn" in col.lower()]
    if req_columns:
        raise ValueError(f"The SSN has been leaked" 
                         f"The leaked ssn details are {req_columns}") 
    return df 

##### Defining the silver table
 
@dp.table(name="silver_transactions_table",comment="After staging the data in the bronze layer now the required transformations have to be applied on the data in adherence to the requirements specified",table_properties={"delta.enableChangeDataFeed": "true"}) 

def silver_transactions_table():
    df1= spark.readStream.table("resillient_bronze_ingestion")
    good_records= df1.filter((f.col("HospitalID").isNotNull())&(f.col("_rescuedDataColumn").isNull()))
    good_records=good_records.withColumn("Sensitive_PII",f.sha2(f.concat(f.col("SSN"),f.lit(salt_key)),256))
    good_records= good_records.drop(f.col("SSN"))
    good_records= good_records.select(f.col("HospitalID").astype("int"),f.col("Name").astype("string"),f.col("Sensitive_PII").astype("string"),f.col("Address").astype("string"),f.col("Phone").astype("string"),f.col("Age").astype("int"),f.col("LastVisitDate").astype("date"),f.col("ZipCode").astype("string"))
    
    good_records=leak_protection(good_records)
    return good_records

### Create a View that explicitly excludes the Hashed_SSN for "General Analytics" users.
@dp.view(name="analytical_view", comment="Here we have to stimulate and create a view that explicitly excludes the Hashed_SSN columns for General Analyticsusers.")

def analytical_view():
    df = spark.read.table("silver_transactions_table")
    df=df.select(f.col("HospitalID"),f.col("Name"),f.col("Address"),f.col("Phone"),f.col("Age"),f.col("LastVisitDate"),f.col("ZipCode"))
    return df

#### Defining the complex buisness logics on the silver table that is created 
@dp.view(name="silver_transactions_updates",comment="After the data has been mildly transformed in the staging silver table, we now have to build the final layout of the silver table with the implementation of the complex business logics")

def silver_transactions_updates():
    df=spark.readstream.table("silver_transactions_table").option("readChangeFeed", "true")
    from pyspark.sql.window import Window
    window_created= Window.partitionBy(f.col("HospitalID"),f.col("SourceUpdateTimstamp"))
    df=df.withColumn("rn",row_number().over(window_created))
    df=df.filter(f.col("rn")==1)
    df=df.drop(f.col("rn"))
    return df

dp.create_streaming_table(name="final_silver_transactions")

dp.create_auto_cdc_flow(target="final_silver_transactions",source="silver_transactions_updates",
keys=["PatientID"],sequence_by=struct(col("LastVisitDate"),col("SourceUpdateTimestamp")),stored_as_scd_type=2,track_history_column_list=["Address"])

dp.create_auto_cdc_flow(target="final_silver_transactions",source="silver_transactions_updates",keys=["PatientID"],sequence_by=struct(col("LastVisitDate"),col("SourceUpdateTimestamp")),stored_as_scd_type=1,track_history_column_list=["Phone"])

    
