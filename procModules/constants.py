CLUSTER_NAME = "shg01"
PORT = ":9000"
DATA_PATH = "hdfs://" + CLUSTER_NAME+PORT + "/test"
#DATA_PATH = "file://./test"
MAPFILE_PATH = "hdfs://" + CLUSTER_NAME + PORT + "/oedo/sh13/mapfiles"
#MAPFILE_PATH = "file://./map_files"
PRMFILE_PATH = "hdfs://" + CLUSTER_NAME + PORT + "/oedo/sh13/prm"
#MAPFILE_PATH = "file://./prm"
BATCH_SIZE = 30000
ID_COLNAME = "event_id"
#ID_COLNAME = "ts"