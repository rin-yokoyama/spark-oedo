from pyspark.sql.functions import DataFrame
from pyspark.ml.feature import Bucketizer
import matplotlib.pyplot as plt
import numpy as np

def Hist1D(dataFrame: DataFrame, colName: str, nbins: int, range: tuple[float, float]) -> plt:
    """
    Plot 1D histogram of the column named "colName". Assuming the column stores a value per row

    Parameters
    ----------
    dataFrame: Input DataFrame that contains a column named colName
    colName: Column name to plot
    nbis: Number of bins
    range: Histogram range as [min, max]

    Returns
    -------
    1D histogram as matplotlib.pyplot.plt
    """
    # Step 1: Define bin edges (100 bins between -100 and 100)
    bin_edges = np.linspace(range[0], range[1], num=nbins).tolist()

    # Step 2: Create a Bucketizer
    bucketizer = Bucketizer(splits=bin_edges, inputCol=colName, outputCol="bin")

    # Step 3: Apply the Bucketizer to the DataFrame
    binned_df = bucketizer.transform(dataFrame.select(colName))

    # Step 4: Group by bin and count the occurrences in each bin and remove Null bins
    histogram_df = binned_df.groupBy("bin").count().orderBy("bin")
    histogram_df = histogram_df.filter(histogram_df["bin"].isNotNull())

    # Step 5: Collect the histogram data from Spark to local
    histogram_data = histogram_df.collect()

    # Step 6: Calculate bin centers (optional, for a better x-axis representation)
    bin_edges = np.linspace(range[0], range[1], num=nbins+1)
    counts = np.zeros(len(bin_edges)-1)

    # Step 7: Populate the counts array based on the histogram data
    for row in histogram_data:
        bin_index = int(row['bin'])  # Get the bin index
        counts[bin_index] = row['count']  # Populate the count for the bin

    # Step 8: Calculate bin centers
    bin_centers = 0.5 * (bin_edges[:-1] + bin_edges[1:])

    # Step 9: Plot the histogram
    plt.bar(bin_centers, counts, width=bin_centers[1] - bin_centers[0])

    # Add labels and title
    plt.xlabel(colName)
    plt.ylabel("Frequency")
    plt.title("Hist1D " + colName + " values")

    return plt