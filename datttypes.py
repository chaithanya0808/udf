from pyspark.sql.functions import expr

def transform_structs_with_cast_and_keep(array_column, target_data_types):
    """
    Apply transformation to an array of structs where specific columns are casted to target data types,
    and the remaining columns keep their original data types.

    Args:
        array_column (str): Name of the array column containing structs.
        target_data_types (dict): Dictionary specifying target data types for specific columns.

    Returns:
        pyspark.sql.Column: Expression for transforming the array of structs.
    """
    all_columns = set(target_data_types.keys())  # Include all specified columns
    struct_fields = df.schema[array_column].dataType.fields
    remaining_columns = all_columns.union(field.name for field in struct_fields)  # Include all remaining columns

    cast_expr = ", ".join([
        f"cast(x.{col} as {dtype.simpleString()}) as {col}"
        if col in target_data_types else f"x.{col}"  # Keep the column as is
        for col, dtype in target_data_types.items()
    ])

    return expr(f"transform({array_column}, x -> struct({cast_expr}))")


# Example usage
target_data_types = {
    "a": IntegerType(),
    "b": DecimalType(10, 2),
    "c": TimestampType()
}

df_transformed = df.withColumn("array_of_structs", transform_structs_with_cast_and_keep("array_of_structs", target_data_types))
df_transformed.show(truncate=False)
df_transformed.printSchema()
