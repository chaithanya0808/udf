from pyspark.sql.functions import expr
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

    # Create the transformation expression
    cast_exprs = [
        f"cast(x.{field.name} as {target_data_types[field.name].simpleString()}) as {field.name}"
        if field.name in target_data_types
        else f"x.{field.name} as {field.name}"
        for field in struct_type.fields
    ]

    transform_expr = f"transform({array_column}, x -> struct({', '.join(cast_exprs)}))"
    return df.withColumn(array_column, expr(transform_expr))

# Example usage
target_data_types = {
    "a": IntegerType(),
    "b": DecimalType(10, 2),
    "c": TimestampType()
}

# Example DataFrame creation
data = [
    (1, [{"a": "1", "b": "2.0", "c": "2023-01-01 00:00:00", "d": "keep_this"}, {"a": "3", "b": "4.0", "c": "2023-01-02 00:00:00", "d": "keep_this"}]),
    (2, [{"a": "5", "b": "6.0", "c": "2023-01-03 00:00:00", "d": "keep_this"}, {"a": "7", "b": "8.0", "c": "2023-01-04 00:00:00", "d": "keep_this"}])
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("array_of_structs", ArrayType(StructType([
        StructField("a", StringType(), True),
        StructField("b", StringType(), True),
        StructField("c", StringType(), True),
        StructField("d", StringType(), True),  # this column will remain unchanged
    ])), True)
])

df = spark.createDataFrame(data, schema)

df_transformed = transform_structs_with_cast_and_keep(df, "array_of_structs", target_data_types)
df_transformed.show(truncate=False)
df_transformed.printSchema()
