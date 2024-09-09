from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, col, arrays_zip, collect_list, DataFrame

def ToFloat(dataFrame: DataFrame, colName: str, p0: float = 0., p1: float = 1.) -> DataFrame:
    """
    Add uniform random numbers (0,1) to the integer column, 'colName' and replace it with
    float column. Scaling and offseting can be added by (p0, p1) arguments.
    
    :param dataFrame: Input DataFrame with the integer columns.
    :param colName: The name of the integer column in dataFrame.
    :param p0: The offset value. Default is '0'.
    :param p1: The scaling value. Default is '1'.
    :return: A DataFrame with the transformed float column, 'colName'.
    """
 
    float_df = dataFrame.withColumn(
        colName,
        p0 + p1 * (F.col(colName) + F.rand())
    )

    return float_df

def linearCalib(input_df: DataFrame, calibration_df: DataFrame, 
                charge_col: str, id_col: str, p0_col: str = "p0", p1_col: str = "p1") -> DataFrame:

    """
    Apply linear calibration to the 'charge' column based on 'id' column and calibration coefficients (p0, p1).
    
    :param input_df: Input DataFrame with the charge and id columns.
    :param calibration_df: DataFrame containing id, p0, and p1 columns for calibration.
    :param charge_col: The name of the charge column in input_df.
    :param id_col: The name of the id column in both DataFrames.
    :param p0_col: The name of the p0 column in calibration_df. Default is 'p0'.
    :param p1_col: The name of the p1 column in calibration_df. Default is 'p1'.
    :return: A DataFrame with the calibrated charge replace column 'charge_col'.
    """
    # Join the input data with the calibration data on the 'id' column
    joined_df = input_df.join(calibration_df, on=id_col, how="left")
    
    # Apply the linear calibration: y = p0 + p1 * charge
    calibrated_df = joined_df.withColumn(f"transform({charge_col}, x -> {p0_col} + {p1_col} * x )")
    calibrated_df = joined_df.withColumn(F.col(p0_col) + F.col(p1_col) * F.col(charge_col))
    
    return calibrated_df