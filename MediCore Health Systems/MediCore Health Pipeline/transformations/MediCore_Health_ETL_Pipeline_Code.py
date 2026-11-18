### Setting up the environment
from pyspark.sql import functions as f
from pyspark.sql.types import*
from pyspark import pipelines as dp 
from pyspark.sql import SparkSession
import datetime

### Setting up the requirements and creating holders for the data location paths
zip_code_ref_path="/Volumes/medicore_health_catalog/staging/zip_code_data/"
bronze_raw_data="/Volumes/medicore_health_catalog/staging/raw_data/"
bronze_raw_schema_location="/Volumes/medicore_health_catalog/staging/raw_data_schema_location"

### Reading the static dataframe that contains the information of the zip codes. Handle missing Zip codes gracefully ("Unknown").
def read_zip_code_reference():
    zip_code_df= spark.read.format("csv").option("inferSchema","false").option("header","true").load(zip_code_ref_path)
    zip_code_df= zip_code_df.select(f.col("ZipCode").astype("string"),f.col("County").astype("string"),f.col("State").astype("string"))
    zip_code_df= zip_code_df.withColumn("ZipCode",f.when(f.col("ZipCode").isNull(),f.lit("Unknown")).otherwise(f.col("ZipCode")))
    zip_code_df=zip_code_df.withColumn("County",f.when(f.col("County").isNull(),f.lit("Unknown")).otherwise(f.col("County")))
    zip_code_df=zip_code_df.withColumn("State",f.when(f.col("State").isNull(),f.lit("Unknown")).otherwise(f.col("State")))
    return zip_code_df

# Retrieve the secret from the databricks pipeline configuration and raising an error if the salt key is missing
salt_key=spark.conf.get("pii.salt_key",None)
if not salt_key:
    raise ValueError("SSN salt key is missing.")   

### Building the bronze ingestion code with the following requiements specified by the client.
@dp.table(name="resillient_bronze_ingestion",comment="Here we are ingesting our data based on the requirements specified in the above mentioned pointers")

def resillient_bronze_ingestion():
    df=spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemaLocation",bronze_raw_schema_location).option("cloudFiles.inferColumnTypes","false").option("header","true").option("cloudFiles.schemaEvolutionMode","addNewColumns").option("cloudFiles.rescuedDataColumn","_rescued_data").load(bronze_raw_data)
    df=df.withColumn("ingestion_timestamp",f.current_timestamp())
    df=df.withColumn("input_source_file",f.input_file_name())
    return df

### After successful processing, source files must be programmatically moved from Landing to Processed/YYYYMMDD paths. Here we are assuming that all the incoming files have unique names.
@dp.table(name="source_audit_table",comment="After successfull completion of the data reading from source, the source files have to be moved from the landing to the processed layer")
def source_audit_table():
    landing   = "/Volumes/medicore_health_catalog/staging/raw_data/"
    processed = f"/Volumes/medicore_health_catalog/staging/processed/{datetime.date.today():%Y%m%d}/"
    dbutils.fs.mkdirs(processed)
    norm = lambda p: p.replace("dbfs:", "")
    loaded = {norm(r[0]) for r in spark.read.table("medicore_health_catalog.staging.resillient_bronze_ingestion").select("input_source_file").distinct().collect()}
    for fi in dbutils.fs.ls(landing):
    if norm(fi.path) in loaded:          # only move files that actually reached Bronze
        dbutils.fs.mv(fi.path, processed + fi.name)

### Defining the leak-protection function and why it shouldn't accept any other column apart from hashed_ssn

def leak_protection(df):
    req_columns=[col for col in df.columns if "ssn" in col.lower() and col.lower()!="hashed_ssn"]
    if req_columns:
        raise ValueError(f"The SSN has been leaked" 
                         f"The leaked SSN details are: {req_columns}") 
    return df 

### Now we are creating a further refined version of the bronze table so that important transformations can be done after ingesting the data in its raw form. Here we are using a temporary view because we dont want this table to persist as a delta table

@dp.temporary_view(name="refined_resillient_bronze_ingestion",comment="A temporary view to perform important transformations in the bronze delta table")

def refined_resillient_bronze_ingestion():
    df= spark.readStream.table("resillient_bronze_ingestion")
    df=df.withColumn("Patient_SK",f.sha2(f.concat(f.col("HospitalID"),salt_key),256))
    df=df.withColumn("HospitalID",f.expr("try_cast(HospitalID AS INT)"))
    df=df.withColumn("LastVisitDate",f.expr("try_cast(LastVisitDate AS DATE)"))
    df=df.withColumn("Age",f.expr("try_cast(Age AS INT)"))
    df=df.withColumn("Name",f.trim(f.col("Name")))
    df=df.withColumn("Hashed_SSN",f.sha2(f.concat(f.col("SSN"),f.lit("salt_key")),256))
    df=df.drop(f.col("SSN"))
    df=leak_protection(df)
    return df

##### Defining the silver table with the feature of Change Data Feed Enabled
 
@dp.table(name="silver_final_transactions_table",comment="After staging the data in the bronze layer now the required transformations have to be applied on the data in adherence to the requirements specified",table_properties={"delta.enableChangeDataFeed": "true"}) 

@dp.expect_or_drop("valid_hospital_id","HospitalID is not null") ### Apply a Delta CHECK CONSTRAINT to ensure Hospital_ID is never NULL at the storage level.Here at this level, all the records that have the HospitalID null then those will be automatically dropped and the pipelien will continue working

def silver_final_transactions_table():
    df1= spark.readStream.table("refined_resillient_bronze_ingestion")
    good_records=df1.filter((f.col("Age") > 0)&(f.col("Name").isNotNull())&(f.col("Age").isNotNull()))
    good_records=leak_protection(good_records)
    return good_records

## Now only the valid rows go to Silver; Invalid rows (Age < 0 OR Name IS NULL) which we have already achieved above. Now it is tiem to filter the bad records and quarantien them to a Quarantine table.

@dp.table(name="silver_quarantine_table",comment="Here we are quarantining the records that are considered fauly based on the requirements provided")

def silver_quarantine_table():
    df=spark.readStream.table("refined_resillient_bronze_ingestion")
    quarantine_df= df.filter((f.col("Age") < 0)|(f.col("Name").isNull()))
    quarantine_df = quarantine_df.withColumn("Violation_Reason",f.when((f.col("Age") < 0) & (f.col("Name").isNull()),f.lit("Negative_Age & Name Null")).when(f.col("Age") < 0,f.lit("Negative Age")).when(f.col("Name").isNull(),f.lit("Name Null")).when(f.col("Age").isNull(),f.lit("Age Missing")).otherwise(f.lit("Unknown Violation")))
    qurantine_df_count=quarantine_df.count()
    return quarantine_df

### Create a View that explicitly excludes the Hashed_SSN for "General Analytics" users.
@dp.view(name="analytical_view", comment="Here we have to stimulate and create a view that explicitly excludes the Hashed_SSN columns for General Analyticsusers.")

def analytical_view():
    df = spark.read.table("silver_final_transactions_table")
    df=df.select(f.col("Patient_SK"),f.col("HospitalID"),f.col("Name"),f.col("Address"),f.col("Phone"),f.col("Age"),f.col("LastVisitDate"),f.col("ZipCode"),f.col("ingestion_timestamp"),f.col("input_source_file"))
    return df

#### Defining the complex business logics on the silver table that is created 
@dp.view(name="silver_transactions_updates",comment="After the data has been mildly transformed in the steps above, we now have to build the final layout of the silver table with the implementation of the complex business logics")

def silver_transactions_updates():
    df=spark.readStream.table("silver_final_transactions_table").option("readChangeFeed", "true")
    from pyspark.sql.window import Window
    window_created= Window.partitionBy(f.col("Patient_SK"),f.col("SourceUpdateTimestamp")).orderBy(f.col(f.col("SourceUpdateTimestamp")))
    df=df.withColumn("rn",row_number().over(window_created))
    df=df.filter(f.col("rn")==1)
    df=df.drop(f.col("rn"))
    ### Join the stream with a static ZipCode reference table to add County and State  columns. Handle missing Zip codes gracefully ("Unknown").
    zip_code_df_req=read_zip_code_reference()
    final_df= df.alias("a").join(zip_code_df_req.alias("b"),on=f.col("a.ZipCode")==f.col("b.ZipCode"),how="left").drop(f.col("b.ZipCode"))
    final_df=final_df.withColumn("County", f.coalesce(f.col("County"), f.lit("Unknown"))).withColumn("State",f.coalesce(f.col("State"),f.lit("Unknown")))
    return final_df

### Implement a single MERGE logic that handles SCD Type 2 for Address changes  (History) and SCD Type 1 for Phone changes (Overwrite).Ensure that a record with an older LastVisitDate does not accidentally overwrite a  newer record in the target table.
dp.create_streaming_table(name="final_silver_transactions",table_properties={"delta.enableChangeDataFeed": "true"})
dp.create_auto_cdc_flow(target="final_silver_transactions",source="silver_transactions_updates",
keys=["Patient_SK"],sequence_by=struct(f.coalesce(f.col("LastVisitDate"),f.col("SourceUpdateTimestamp"),f.lit("9999-12-31")),stored_as_scd_type=2,track_history_column_list=["Address"]))

dp.create_auto_cdc_flow(target="final_silver_transactions",source="silver_transactions_updates",keys=["Patient_SK"],sequence_by=struct(col("LastVisitDate"),col("SourceUpdateTimestamp")),stored_as_scd_type=1,track_history_column_list=["Phone"])

### Output a "Control Totals" dataframe: Count(Input Rows) vs. Count(Written Rows) +  Count(Quarantined Rows).
@dp.materialized_view(name="controls_totals",comment="This table keeps track of the records that are passed through every layer of the ETL Pipeline")
def conrols_totals():
    input_data_ct=spark.readStream.table("resillient_bronze_ingestion").agg(f.count("*").alias("total_input_records"))
    good_data_ct=spark.readStream.table("silver_final_transactions_table").agg(f.count("*").alias("total_good_records"))
    quarantined_data_ct=spark.readStream.table("silver_quarantine_table").agg(f.count("*").alias("total_bad_records"))
    final_df= input_data_ct.crossJoin(good_data_ct).crossJoin( quarantined_data_ct)
    final_df=final_df.withColumn("reconcillation_column",f.when(f.col("total_input_records")!=(f.col("total_good_records")+f.col("total_bad_records"))),"Mistmatched").otherwise("Matched")
    return final_df

#### Implementing the optimization techniques

dbutils.widgets.text("Patient_SK", "")
FINAL = f"{S}.final_silver_transactions"

## Delta's default target file is ~1 GB; 128 MB suits small tables and selective lookups.
spark.sql(f"ALTER TABLE {FINAL} SET TBLPROPERTIES ('delta.targetFileSize'='128mb')")
spark.sql(f"OPTIMIZE {FINAL} ZORDER BY (HospitalID) SET TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols = "HospitalID")'))


# R-25: forensic restore (inspect before restoring)
display(spark.sql(f"DESCRIBE HISTORY {FINAL}"))
spark.sql(f"RESTORE TABLE {FINAL} TO TIMESTAMP AS OF {(current_timestamp() - INTERVAL 5 MINUTES)}")

# R-10: right to forget
pid = dbutils.widgets.get("patient_id")
for t in ["resillient_bronze_ingestion", "silver_transactions_table",
          "silver_quarantine_table", "final_silver_transactions"]:
    spark.sql(f"DELETE FROM {S}.{t} WHERE PatientID = :pid", args={"pid": pid})
    spark.sql(f"VACUUM {S}.{t}")   # default 7-day retention; files are physically removed once they age out  