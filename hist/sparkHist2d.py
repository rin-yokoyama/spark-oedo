from pyspark.sql.functions import DataFrame, posexplode
from pyspark.ml.feature import Bucketizer
import matplotlib.pyplot as plt
import numpy as np

def Hist2D(dataFrame: DataFrame, colName: tuple[str, str], nbins: tuple[int, int], range: tuple[tuple[float, float], tuple[float, float]]) -> plt:
    """
    Plot 2D histogram of the column named colName["x", "y"]. Assuming the column stores a value per row

    Parameters
    ----------
    dataFrame: Input DataFrame
    colName: Column names to plot [x, y]
    nbis: Number of bins [nbinsx, nbinsy]
    range: Histogram range as [x[min, max], y[min, max]]

    Returns
    -------
    2D histogram as matplotlib.pyplot.plt
    """
    # Step 1: Define bin edges
    bin_edges_x = np.linspace(range[0][0], range[0][1], num=nbins[0]).tolist()
    bin_edges_y = np.linspace(range[1][0], range[1][1], num=nbins[1]).tolist()

    # Step 2: Create a Bucketizer
    bucketizer_x = Bucketizer(splits=bin_edges_x, inputCol=colName[0], outputCol="bin_x")
    bucketizer_y = Bucketizer(splits=bin_edges_y, inputCol=colName[1], outputCol="bin_y")

    # Step 3: Apply the Bucketizer to the DataFrame
    binned_df = bucketizer_x.transform(dataFrame.select(colName[0], colName[1]))
    binned_df = bucketizer_y.transform(binned_df)

    # Step 4: Group by bin and count the occurrences in each bin and remove Null bins
    binned_df = binned_df.filter(binned_df["bin_x"].isNotNull() & binned_df["bin_y"].isNotNull())
    histogram_2d_df = binned_df.groupBy("bin_x","bin_y").count()

    # Step 5: Collect the histogram data from Spark to local
    histogram_data = histogram_2d_df.collect()

    # Step 6: Extract bin indices and counts from the collected data
    counts = np.zeros((len(bin_edges_x) - 1, len(bin_edges_y) - 1))

    # Step 7: Populate the 2D counts array
    for row in histogram_data:
        bin_x = int(row['bin_x'])
        bin_y = int(row['bin_y'])
        counts[bin_x, bin_y] = row['count']

    # Step 8: Plot the 2D histogram using a heatmap
    plt.imshow(counts.T, extent=[range[0][0], range[0][1], range[1][0], range[1][1]], origin='lower', aspect='auto', cmap='viridis')

    # Add labels and title
    plt.xlabel(colName[0])
    plt.ylabel(colName[1])
    plt.title("2D Histogram of " + colName[1] + " vs " + colName[0])

    # Show the color bar
    plt.colorbar(label='Counts')

    return plt

def Hist2DArrays(dataFrame: DataFrame, colName: tuple[str, str], nbins: tuple[int, int], range: tuple[tuple[float, float], tuple[float, float]]) -> plt:
    """
    Plot 2D histogram of the column named colName["x", "y"]. Assuming the column stores an array that is aligned with each other

    Parameters
    ----------
    dataFrame: Input DataFrame
    colName: Column names to plot [x, y]
    nbis: Number of bins [nbinsx, nbinsy]
    range: Histogram range as [x[min, max], y[min, max]]

    Returns
    -------
    2D histogram as matplotlib.pyplot.plt
    """
    dataFrame = dataFrame.select(colName[0],colName[1])
    exploded_pos_x_df = dataFrame.select(posexplode(colName[0]).alias("idx", colName[0]))
    exploded_pos_y_df = dataFrame.select(posexplode(colName[1]).alias("idx", colName[1]))
    exploded_df = exploded_pos_x_df.join(exploded_pos_y_df, "idx")

    return Hist2D(exploded_df, colName, nbins, range)
