from pyspark.sql import SparkSession, functions as F


def convert_datatypes_in_array_of_struct(df, column_name, conversion_logic):
    """
    Dynamically converts data types for columns within an array of struct in a DataFrame.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame.
        column_name (str): The name of the column containing the array of structs.
        conversion_logic (callable): A function that takes a column name within the
            struct as input and returns the desired target data type or a transformation
            function (e.g., F.cast) for that column.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the converted data types.
    """

    struct_field_names = df.schema[column_name].dataType.fields

    # Extract the original struct type
    original_struct_type = StructType([StructField(f.name, f.dataType) for f in struct_field_names])

    # Create a lambda function to handle individual struct elements
    def convert_struct_element(element):
        # Create a new struct with converted data types
        new_struct = F.struct(*[
            F.when(F.lit(True), conversion_logic(col_name)).otherwise(element[col_name])
            for col_name in struct_field_names
        ])
        return new_struct

    # Apply the lambda function using explode and array_except
    return df.withColumn(
        column_name,
        F.array_except(
            F.explode(column_name),
            F.array(
                *[
                    convert_struct_element(F.col("col"))
                    for col in F.col(column_name).cast(ArrayType(original_struct_type))
                ]
            )
        )
    ).withColumn(column_name, F.array(F.col(column_name)))


# Example usage
spark = SparkSession.builder.appName("DataTypeConversion").getOrCreate()

# Sample data (assuming 'data' is an array of structs)
data = [
    ([1, 2.0, "string"], 34, 87, "pending", (65, 22.5)),
    ([3, 4.5, "another"], 56, 98, "completed", (89, 10.1))
]

df = spark.createDataFrame(data, ["data", "payerId", "percentage", "plateNumberStatus", "ratio"])

# Define a conversion logic function (modify as needed)
def convert_data_type(col_name):
    if col_name == "m1":
        return F.cast(col_name, IntegerType())
    elif col_name == "m2":
        return F.cast(col_name, DoubleType())
    else:
        return F.col(col_name)  # Keep other columns unchanged

# Convert data types based on the defined logic
df = convert_datatypes_in_array_of_struct(df, "data", convert_data_type)

df.printSchema()
df.show()
