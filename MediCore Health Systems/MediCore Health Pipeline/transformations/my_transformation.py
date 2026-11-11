### Setting up the environment
from pyspark.sql import functions as f
from pyspark.sql.types import*
from pyspark import pipelines as dp 
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("MediCore Health").getOrCreate()

### Reading the static dataframe that contaisn the information of the zip codes
zip_code_df= spark.read.format("csv").option("inferSchema","false").load("/Volumes/medicore_health_catalog/staging/zip_code_data/")
zip_code_df= zip_code_df.select(f.col("ZipCode").astype("string"),f.col("County").astype("string"),f.col("State").astype("string"))
zip_code_df= zip_code_df.withColumn("ZipCode",f.when(f.col("ZipCode").isNull(),f.lit("Unknown")).otherwise(f.col("ZipCode")))

### Building the ingestion code with the following requiements specified by the client.
@dp.table(name="resillient_bronze_ingestion",comment="Here we are ingesting our data based on the requirements specified in the above mentioned pointers")

def resillient_bronze_ingestion():
    df=spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemaLocation","/Volumes/medicore_health_catalog/staging/raw_data_schema_location").option("cloudFiles.inferColumnTypes","false").option("header","true").option("cloudFiles.schemaEvolutionMode","addNewColumns").option("cloudFiles.rescuedDataColumn","_rescuedDataColumn").load("/Volumes/medicore_health_catalog/staging/raw_data/").withColumn("ingestion_timestamp",f.current_timestamp()).withColumn("file_name",f.input_file_name())
    
    df= df.withColumn("HospitalID",f.col("HospitalID").try_cast("integer"))
    df=df.withColumn("LastVisitDate",f.col("LastVisitDate").try_cast("date"))
    input_rows_count= df.count()
    return df
    return input_rows_count

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

@dp.expect_or_drop("valid_hospital_id","HospitalID is not null")

def silver_transactions_table():
    df1= spark.readStream.table("resillient_bronze_ingestion")
    good_records= df1.filter((f.col("HospitalID").isNotNull())&(f.col("_rescuedDataColumn").isNull()))
    good_records=good_records.withColumn("Sensitive_PII",f.sha2(f.concat(f.col("SSN"),f.lit(salt_key)),256))
    good_records= good_records.drop(f.col("SSN"))
    good_records= good_records.select(f.trim(f.col("HospitalID")).astype("int"),f.trim(f.col("Name")).astype("string"),f.trim(f.col("Sensitive_PII")).astype("string"),f.trim(f.col("Address")).astype("string"),f.trim(f.col("Phone")).astype("string"),f.trim(f.col("Age")).astype("int"),f.col("LastVisitDate").astype("date"),f.trim(f.col("ZipCode")).astype("string"))
    good_records=good_records.filter((f.col("Age") > 0)&(f.col("Name").isNotNull()))
    good_records=good_records.withColumn("ZipCode",f.when(f.col("ZipCode").isNull(),f.lit("Unknown")).otherwise(f.col("ZipCode")))
    good_records=leak_protection(good_records)
    written_rows=good_records.count()
    return good_records
    return written_rows

### Create a View that explicitly excludes the Hashed_SSN for "General Analytics" users.
@dp.view(name="analytical_view", comment="Here we have to stimulate and create a view that explicitly excludes the Hashed_SSN columns for General Analyticsusers.")

def analytical_view():
    df = spark.read.table("silver_transactions_table")
    df=df.select(f.col("HospitalID"),f.col("Name"),f.col("Address"),f.col("Phone"),f.col("Age"),f.col("LastVisitDate"),f.col("ZipCode"))
    return df

#### Defining the complex business logics on the silver table that is created 
@dp.view(name="silver_transactions_updates",comment="After the data has been mildly transformed in the staging silver table, we now have to build the final layout of the silver table with the implementation of the complex business logics")

def silver_transactions_updates():
    df=spark.readStream.table("silver_transactions_table").option("readChangeFeed", "true")
    from pyspark.sql.window import Window
    window_created= Window.partitionBy(f.col("HospitalID"),f.col("SourceUpdateTimstamp"))
    df=df.withColumn("rn",row_number().over(window_created))
    df=df.filter(f.col("rn")==1)
    df=df.drop(f.col("rn"))
    ### Generate a unique, persistent Patient_SK for every record. Use a deterministic  generation method (e.g., hashing Business Keys).
    df=df.withColumn("Patient_SK",f.sha2(f.concat_ws("||",f.col("HospitalID"),f.col("LastVisitDate")),256))
    ### Join the stream with a static ZipCode reference table to add County and State  columns. Handle missing Zip codes gracefully ("Unknown").
    final_df= df.alias("a").join(zip_code_df.alias("b"),on=f.col("a.ZipCode")==f.col("b.ZipCode"),how="left").drop(f.col("b.ZipCode"))
    return final_df

### Valid rows go to Silver; Invalid rows (Age < 0 OR Name IS NULL)  go to a Quarantine table.
def silver_quarantine_table():
    df=spark.readStream.table("silver_transactions_table").option("readChangeFeed", "true")
    quarantine_df= df.filter((f.col("Age") <= 0)|(f.col("Name").isNull()))
    quarantine_df = quarantine_df.withColumn("Violation_Reason",f.when((f.col("Age") <= 0) & (f.col("Name").isNull()),f.lit("Negative_Age & Name Null")).when(f.col("Age") <= 0,f.lit("Negative Age")).when(f.col("Name").isNull(),f.lit("Name Null")).otherwise(f.lit("Unknown Violation")))
    quanratine_df_count=quarantine_df.count()
    return quarantine_df
    return quarantine_df_count

### Implement a single MERGE logic that handles SCD Type 2 for Address changes  (History) and SCD Type 1 for Phone changes (Overwrite).Ensure that a record with an older LastVisitDate does not accidentally overwrite a  newer record in the target table.
dp.create_streaming_table(name="final_silver_transactions")
dp.create_auto_cdc_flow(target="final_silver_transactions",source="silver_transactions_updates",
keys=["PatientID"],sequence_by=struct(col("LastVisitDate"),col("SourceUpdateTimestamp")),stored_as_scd_type=2,track_history_column_list=["Address"])

dp.create_auto_cdc_flow(target="final_silver_transactions",source="silver_transactions_updates",keys=["PatientID"],sequence_by=struct(col("LastVisitDate"),col("SourceUpdateTimestamp")),stored_as_scd_type=1,track_history_column_list=["Phone"])

### Output a "Control Totals" dataframe: Count(Input Rows) vs. Count(Written Rows) +  Count(Quarantined Rows).
data=[(input_rows_count,written_rows,quarantine_df_count)]
reconcillation_df=spark.createDataFrame(data,["Input_Rows","Written_Rows","Quarantine_Rows"])
