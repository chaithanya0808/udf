# pyspark,  i have a multiple list and each list has columns name associated with firstclass column.   i have a pyspark dataframe and pivot one of the column . once  pivot is done i have to compare pivot columns which is transposed dataframe and compare columns in the lists and create a jsonb struct for matched column associated with particular list from transposed column and consider when column matched   populate with the value and when column is not matched from list ignore it and when column names in the list is not present in pivot columns make it as none for future handling, give an example and code for same

# Pivot the DataFrame: Pivot the DataFrame based on a specific column.
# Transpose the Pivot DataFrame: Transpose the pivot DataFrame to compare the columns.
# Compare columns: Compare the columns of the transposed DataFrame with the columns in the list.
# Create JSONB struct: Create a JSONB struct for matched columns from the list with their respective values in the transposed DataFrame.
# Handle missing columns: For columns in the list that are not present in the transposed DataFrame, set their values to None.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, struct, collect_list, udf
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Column Comparison and JSONB Creation") \
    .getOrCreate()

# Sample data for demonstration
data = [("A", 1), ("B", 2), ("C", 3)]
columns_list = [["firstclass", "secondclass"], ["firstclass", "thirdclass"]]

# Create DataFrame
df = spark.createDataFrame(data, ["class", "value"])

# Step 1: Pivot the DataFrame
pivot_df = df.groupBy("class").pivot("class").agg(collect_list("value")).na.fill(0)

# Step 2: Transpose the Pivot DataFrame
transpose_df = pivot_df.select(*([lit(col(col_name)).alias("class")] + [col(col_name) for col_name in pivot_df.columns[1:]]))

# Step 3: Compare columns
matched_columns = []
for lst in columns_list:
    for col_name in lst:
        if col_name in transpose_df.columns:
            matched_columns.append(col_name)

# Step 4: Create JSONB struct
def create_json(*args):
    json_dict = {}
    for i in range(0, len(args), 2):
        col_name = args[i]
        value = args[i + 1]
        json_dict[col_name] = value
    return json_dict

create_json_udf = udf(create_json, StringType())

json_struct = create_json_udf(*[col(col_name) for col_name in matched_columns])

# Step 5: Handle missing columns
for col_name in matched_columns:
    df = df.withColumn(col_name, when(col_name.isin(transpose_df.columns), col_name).otherwise(None))

# Show the result
df.show(truncate=False)


# This code should give you a DataFrame with columns populated with their respective values from the transposed DataFrame, and None for columns not present in the transposed DataFrame. The json_struct variable holds the JSONB struct you require. You can further process or save this DataFrame as per your requirements.
