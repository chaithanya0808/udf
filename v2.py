from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, struct, collect_list, when, udf
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
transpose_df = pivot_df.select(*([lit(col_name).alias("class")] + [pivot_df[col_name] for col_name in pivot_df.columns[1:]]))

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

json_struct = create_json_udf(*[transpose_df[col_name] for col_name in matched_columns])

# Step 5: Handle missing columns
for col_name in matched_columns:
    df = df.withColumn(col_name, when(col_name.isin(transpose_df.columns), col_name).otherwise(None))

# Show the result
df.show(truncate=False)
