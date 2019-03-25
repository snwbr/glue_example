import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# catalog: database and table names
db_name = "happy_db"
db_name_hpi = "hpi"
tbl_comments = "comments"
tbl_demographics = "hpdb_public_hpdb_demographic"
tbl_country_mapping = "hpdb_public_hpdb_country_mapping"
tbl_hpi = "hpi_data_csv"
redshift_db = 'rdhappydb'
redshift_table = 'happydb_full_info'
redshift_table_hpi = 'hpi_full_info'
redshift_user = 'user'
redshift_pass = 'pass'


# output s3 and temp directories
output_dir = "s3://gorilla-etl-rawdata/join"

# Create dynamic frames from the source tables 
comments = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_comments)
demographics = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_demographics)
country_mapping = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_country_mapping)
hpi_index = glueContext.create_dynamic_frame.from_catalog(database=db_name_hpi, table_name=tbl_hpi)

# We're joining the country field with an 3 character acronymn, i.e. USA
country_demographics = Join.apply(demographics, country_mapping, 'country', 'alpha3')

# Now, we join all happydb data togheter
full_data = Join.apply(comments, country_demographics, 'wid', 'wid')

# Writing DynamicFrame contents to Redshift.
glueContext.write_dynamic_frame.from_jdbc_conf(frame = full_data,catalog_connection = "Glue2Redshift",
	connection_options = {"dbtable": redshift_table, "database": redshift_db, "user": redshift_user, "password": redshift_pass},
	redshift_tmp_dir = output_dir + "/temp-dir/")

glueContext.write_dynamic_frame.from_jdbc_conf(frame = hpi_index,catalog_connection = "Glue2Redshift",
	connection_options = {"dbtable": redshift_table_hpi, "database": redshift_db, "user": redshift_user, "password": redshift_pass},
	redshift_tmp_dir = output_dir + "/temp-dir/")
