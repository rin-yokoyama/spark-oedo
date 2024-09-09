from pyspark.sql.functions import DataFrame, window, col

WATERMARK_WINDOW = "10 seconds"
WATERMARK_TS_COL = "stream_uts"
