from pyspark.sql.types import ArrayType

def transform_structs_with_cast_and_keep(df, array_column, target_data_types):
    """
    Apply transformation to an array of structs where specific columns are casted to target data types,
    and the remaining columns keep their original data types.

    Args:
        df (pyspark.sql.DataFrame): DataFrame containing the array column.
        array_column (str): Name of the array column containing structs.
        target_data_types (dict): Dictionary specifying target data types for specific columns.

    Returns:
        pyspark.sql.Column: Expression for transforming the array of structs.
    """
    struct_type = df.select(array_column).schema[array_column].dataType.elementType
    cast_exprs = []

    for col in struct_type.names:
        if col in target_data_types:
            cast_exprs.append(f"cast(x.{col} as {target_data_types[col]}) as {col}")
        else:
            cast_exprs.append(f"x.{col}")

    return expr(f"transform({array_column}, x -> struct({', '.join(cast_exprs)}))")


# Example usage
target_data_types = {
    "a": "integer",
    "b": "decimal(10,2)",
    "c": "timestamp"
}

df_transformed = df.withColumn("array_of_structs", transform_structs_with_cast_and_keep(df, "array_of_structs", target_data_types))
df_transformed.show(truncate=False)
df_transformed.printSchema()
