#SERVER_TYPE = "local"
#SERVER_TYPE = "aws"
SERVER_TYPE = "cluster"

if SERVER_TYPE == "local":
    MASTER = "local[*]"
    DATA_PATH = "file:///home/ryokoyam/spark-oedo/scratch"
    MAPFILE_PATH = "file:///home/ryokoyam/spark-oedo/map_files"
    PRMFILE_PATH = "file:///home/ryokoyam/spark-oedo/prm"

elif SERVER_TYPE == "aws":
    DATA_PATH = "s3://data-analysis"
    MAPFILE_PATH = "s3://spark-oedo/spark-oedo/map_files"
    PRMFILE_PATH = "s3://spark-oedo/spark-oedo/prm"

elif SERVER_TYPE == "cluster":
    CLUSTER_NAME = "shg01"
    PORT = ":9000"
    MASTER = "spark://" + CLUSTER_NAME + ":7077"
    DATA_PATH = "hdfs://" + CLUSTER_NAME+PORT + "/test"
    MAPFILE_PATH = "hdfs://" + CLUSTER_NAME + PORT + "/oedo/sh13/mapfiles"
    PRMFILE_PATH = "hdfs://" + CLUSTER_NAME + PORT + "/oedo/sh13/prm"

BATCH_SIZE = 30000
#ID_COLNAME = "ts"
ID_COLNAME = "event_id"