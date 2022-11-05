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

    tgt_list = app_conf['target_list']
    staging_loc = app_conf['staging_loc']

    for tgt in tgt_list:
        tgt_conf = app_conf[tgt]

        if tgt == 'REGIS_DIM':
            # read the CP file from s3
            df = spark.read.parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + '/' + staging_loc + "/" + tgt_conf['source_data'] + "/")
            df.show()
            df.createOrReplaceTempView(tgt_conf['source_data'])
            res_df = spark.sql(tgt_conf['loadingQuery'])
            res_df.show()


    # create a temp view ontop of that
    # execute the loadingQuery and result a df
    # write the above df to redshift

    # spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" com/test/target_data_loading.py
