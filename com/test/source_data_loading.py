from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import yaml
import os.path
import com.utils.utilities as ut

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .config('spark.jars.packages', 'com.springml:spark-sftp_2.11:1.1.1') \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"]) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    src_list = app_conf['source_list']
    staging_loc = app_conf['staging_loc']
    for src in src_list:
        src_conf = app_conf[src]

        if src == 'SB':
            txn_df = ut.read_from_mysql(app_secret['mysql_conf'],
                                        src_conf["mysql_conf"]["dbtable"],
                                        src_conf["mysql_conf"]["partition_column"],
                                        spark)
            txn_df = txn_df.withColumn('ins_dt', current_date())
            txn_df.write \
                .mode('append') \
                .partitionBy('ins_dt')\
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + '/' + staging_loc + "/" + src)

        if src == 'OL':
           print(src_conf["sftp_conf"]['directory'])
           ol_df  = ut.read_from_sftp(app_secret['sftp_conf'],
                                      src_conf["sftp_conf"]['directory'] + "/*",
                                      os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"]),
                                      spark)
           ol_df = ol_df.withColumn('ins_dt', current_date())
           ol_df.show()
           ol_df.write \
               .mode('append')\
               .partitionBy('ins_dt') \
               .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + '/' + staging_loc + "/" + src)

        if src == 'ADDR':
            addr_df = ut.read_from_mongo(src_conf['mongodb_config'],spark)
            addr_df = addr_df.withColumn('ins_dt', current_date())
            addr_df.show()
            addr_df.write \
                .mode('append') \
                .partitionBy('ins_dt') \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + '/' + staging_loc + "/" + src)

        if src == 'CP':
            fin_schema = StructType() \
                .add("REGIS_CNTRY_CODE", StringType(), True)
                """ .add("REGIS_CTY_CODE", IntegerType(), True) \
                .add("REGIS_ID", IntegerType(), True) \
                .add("REGIS_LTY_ID", IntegerType(), True) \
                .add("REGIS_CNSM_ID", IntegerType(), True) \
                .add("REGIS_DATE", StringType(), True) \
                .add("REGIS_TIME", StringType(), True) \
                .add("REGIS_CHANNEL", StringType(), True) \
                .add("REGIS_GENDER", StringType(), True) \
                .add("REGIS_CITY", StringType(), True) \
                .add("CHILD_ID", IntegerType(), True) \
                .add("CHILD_NB", IntegerType(), True) \
                .add("CHILD_GENDER", StringType(), True) \
                .add("CHILD_DOB", StringType(), True) \
                .add("CHILD_DECEASED", StringType(), True) """
            cp_df = ut.read_from_s3(src_conf,fin_schema,spark)
            cp_df = cp_df.withColumn('ins_dt', current_date())
            cp_df.show()
            cp_df.write \
                .mode('append') \
                .partitionBy('ins_dt') \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + '/' + staging_loc + "/" + src)

# spark-submit --packages "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1" com/test/source_data_loading.py

