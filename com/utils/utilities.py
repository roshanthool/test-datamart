import os.path
def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["hostname"]
    port = mysql_config["port"]
    database = mysql_config["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)

def read_from_mysql(mysql_secret, dbtable, partition_column, spark):
    print("\nReading data from MySQL DB,")
    jdbc_params = {"url": get_mysql_jdbc_url(mysql_secret),
                   "lowerBound": "1",
                   "upperBound": "100",
                   "dbtable": dbtable,
                   "numPartitions": "2",
                   "partitionColumn": partition_column,
                   "user": mysql_secret["username"],
                   "password": mysql_secret["password"]
                   }
    df = spark \
        .read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .options(**jdbc_params) \
        .load()
    return df

def read_from_sftp(sftp_secret, filename):
    ol_txn_df = spark.read \
        .format("com.springml.spark.sftp") \
        .option("host", sftp_secret["hostname"]) \
        .option("port", sftp_secret["port"]) \
        .option("username",sftp_secret["username"]) \
        .option("pem", os.path.abspath(dir + "/../../" + sftp_secret["pem"])) \
        .option("fileType", "csv") \
        .option("delimiter", "|") \
        .load(filename)
    return ol_txn_df

def read_from_mongo(mng_conf):
    addr_df = spark \
        .read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", mng_conf["mongodb_config"]["database"]) \
        .option("collection", mng_conf["mongodb_config"]["collection"]) \
        .load()
    return addr_df

def read_from_s3(s3_conf,fin_schema,spark):
    cp_df = spark.read \
        .option("header", "false") \
        .option("delimiter", ",") \
        .format("csv") \
        .schema(fin_schema) \
        .load("s3a://" + s3_conf["s3_conf"]["s3_bucket"] + "/finances.csv")
    return cp_df
