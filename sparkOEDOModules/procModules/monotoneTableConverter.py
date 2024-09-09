import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import pandas_udf
from sparkOEDOModules.procModules import constants
from typing import Callable

def getConverterUDF(spark: SparkSession, fileName: str) -> Callable[[Column],Column]:
    """
    Create a pandas udf from a monotone table csv

    Parameters
    ----------
    spark: Spark Session
    fileName: CSV file name under prm/

    Returns
    -------
    pandas UDF
    example:
        converter_udf = getConverterUDF(spark, "table.csv")
        df.withColumn("converted", converter_udf(df["X"]))
    """
    # Step 1: Read the CSV with "x" and "y" columns into a Pandas DataFrame
    csv_df = spark.read.csv(constants.PRMFILE_PATH + f"/{fileName}",header=True, inferSchema=True)
    csv_df = csv_df.orderBy("x").dropDuplicates(["y"])

    # Step 2: Create an interpolation function from the CSV data
    x_csv = np.array(csv_df.select("x").collect())
    y_csv = np.array(csv_df.select("y").collect())
    x_csv = np.ravel(x_csv)
    y_csv = np.ravel(y_csv)
    interp_func = np.interp(x_csv, y_csv)

    # Step 3: Register the interpolation function as a pandas_udf
    @pandas_udf("double")  # UDF that returns double values (for yvalues)
    def interpolate_yvalues_udf(xvalues: pd.Series) -> pd.Series:
        return pd.Series(interp_func(xvalues))

    return interpolate_yvalues_udf
