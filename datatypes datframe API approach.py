from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, expr
from pyspark.sql.types import IntegerType, DecimalType, TimestampType

def transform_structs_with_cast_and_keep(df, array_column, target_data_types):
    """
    Apply transformation to an array of structs where specific columns are casted to target data types,
    and the remaining columns keep their original data types.

    Args:
        df (pyspark.sql.DataFrame): DataFrame containing the array column.
        array_column (str): Name of the array column containing structs.
        target_data_types (dict): Dictionary specifying target data types for specific columns.

    Returns:
        pyspark.sql.DataFrame: DataFrame with the transformed array of structs.
    """
    struct_type = df.schema[array_column].dataType.elementType

    # Generate expressions for casting each field as necessary
    cast_fields = [
        col(f"{array_column}.{field.name}").cast(target_data_types[field.name]).alias(field.name)
        if field.name in target_data_types
        else col(f"{array_column}.{field.name}").alias(field.name)
        for field in struct_type.fields
    ]

    # Apply the transformation using `withColumn` and `transform`
    df_transformed = df.withColumn(
        array_column,
        expr(f"transform({array_column}, x -> struct({', '.join([f'x.{field.name}' for field in struct_type.fields])}))")
    )

    for field in struct_type.fields:
        if field.name in target_data_types:
            df_transformed = df_transformed.withColumn(
                f"{array_column}.{field.name}", col(f"{array_column}.{field.name}").cast(target_data_types[field.name])
            )

    return df_transformed

# Example usage
if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("Example").getOrCreate()

    # Define the target data types for specific columns
    target_data_types = {
        "a": IntegerType(),
        "b": DecimalType(16, 2),
        "c": TimestampType(),
        "d": IntegerType()
    }

    # Example DataFrame creation
    data = [
        (1, [{"a": "1", "b": "2.0", "c": "2023-01-01 00:00:00", "d": "10"}, {"a": "3", "b": "4.0", "c": "2023-01-02 00:00:00", "d": "20"}]),
        (2, [{"a": "5", "b": "6.0", "c": "2023-01-03 00:00:00", "d": "30"}, {"a": "7", "b": "8.0", "c": "2023-01-04 00:00:00", "d": "40"}])
    ]

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("array_of_structs", ArrayType(StructType([
            StructField("a", StringType(), True),
            StructField("b", StringType(), True),
            StructField("c", StringType(), True),
            StructField("d", StringType(), True),
        ])), True)
    ])

    df = spark.createDataFrame(data, schema)

    # Transform the DataFrame
    df_transformed = transform_structs_with_cast_and_keep(df, "array_of_structs", target_data_types)

    # Show the transformed DataFrame
    df_transformed.show(truncate=False)
    df_transformed.printSchema()

    # Stop the Spark session
    spark.stop()

