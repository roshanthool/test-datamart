from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType
import yaml
import os.path
import com.utils.utilities as ut

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    src_list = app_conf['source_list']
    staging_loc = app_conf['staging_loc']
    for src in src_list:
        src_conf = app_conf[src]
        if src == 'SB':
            print(app_secret['mysql_conf'])
            print(src_conf["mysql_conf"])
            txn_df = ut.read_from_mysql(app_secret['mysql_conf'],
                                        src_conf["mysql_conf"]["dbtable"],
                                        src_conf["mysql_conf"]["partition_column"],
                                        spark)
            txn_df = txn_df.withColumn('ins_dt', current_date())

            txn_df.show()
            txn_df.write \
                .mode('append') \
                .partitionBy('ins_dt')\
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + '/' + staging_loc + "/" + src)

        if src == 'OL':
           ol_df  = ut.read_from_sftp(app_secret['sftp_conf'],
                                      current_dir + '/' + src_conf["sftp_conf"]['directory'])
           ol_df = ol_df.withColumn('ins_dt', current_date())
           ol_df.show()
           ol_df.write \
               .mode('append')\
               .partitionBy('ins_dt') \
               .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + '/' + staging_loc + "/" + src)

        if src == 'ADDR':
            addr_df = ut.read_from_mongo(app_conf['mongodb_config'])
            addr_df = addr_df.withColumn('ins_dt', current_date())
            addr_df.show()
            addr_df.write \
                .mode('append') \
                .partitionBy('ins_dt') \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + '/' + staging_loc + "/" + src)

        if src == 'CP':
            fin_schema = StructType() \
                .add("id", IntegerType(), True) \
                .add("has_debt", BooleanType(), True) \
                .add("has_financial_dependents", BooleanType(), True) \
                .add("has_student_loans", BooleanType(), True) \
                .add("income", DoubleType(), True)
            cp_df = ut.read_from_s3(src_conf,fin_schema,spark)
            cp_df = cp_df.withColumn('ins_dt', current_date())
            cp_df.show()
            cp_df.write \
                .mode('append') \
                .partitionBy('ins_dt') \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + '/' + staging_loc + "/" + src)

# spark-submit --packages "mysql:mysql-connector-java:8.0.15" com/test/source_data_loading.py
