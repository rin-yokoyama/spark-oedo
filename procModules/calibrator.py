from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, col, arrays_zip, collect_list, DataFrame

def ToFloat(dataFrame: DataFrame, colName: str) -> DataFrame:

    float_df = dataFrame.withColumn(
        colName,
        F.expr("transform("+colName+", x -> x + cast(rand() as double))")
    )

    return F.broadcast(float_df)

def linearCalib(input_df: DataFrame, calibration_df: DataFrame, 
                charge_col: str, id_col: str, p0_col: str = "p0", p1_col: str = "p1") -> DataFrame:
    """
    Apply linear calibration to the 'charge' column based on 'id' and calibration coefficients (p0, p1).
    
    :param input_df: Input DataFrame with the charge and id columns.
    :param calibration_df: DataFrame containing id, p0, and p1 columns for calibration.
    :param charge_col: The name of the charge column in input_df.
    :param id_col: The name of the id column in both DataFrames.
    :param p0_col: The name of the p0 column in calibration_df. Default is 'p0'.
    :param p1_col: The name of the p1 column in calibration_df. Default is 'p1'.
    :return: A DataFrame with the calibrated charge column added.
    """
    # Join the input data with the calibration data on the 'id' column
    joined_df = input_df.join(calibration_df, on=id_col, how="left")
    
    # Apply the linear calibration: y = p0 + p1 * charge
    calibrated_df = joined_df.withColumn(charge_col + "_cal", F.expr(f"{p0_col} + {p1_col} * {charge_col}"))
    
    return calibrated_df