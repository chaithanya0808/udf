from pyspark.sql import SparkSession, functions as F


def transform_structs_with_cast_and_keep(df, array_column, target_data_types):
  """
  Apply transformation to an array of structs where specific columns are cast to target data types,
  and the remaining columns keep their original data types.

  Args:
    df (pyspark.sql.DataFrame): DataFrame containing the array column.
    array_column (str): Name of the array column containing structs.
    target_data_types (dict): Dictionary specifying target data types for specific columns.

  Returns:
    pyspark.sql.DataFrame: DataFrame with the transformed array of structs.
  """
  struct_fields = df.schema[array_column].dataType.elementType.fields

  # Create expressions for casting and preserving columns
  cast_exprs = [
      F.when(F.col(f"{array_column}.{field.name}").isNotNull(),
            F.cast(F.col(f"{array_column}.{field.name}"), target_data_types.get(field.name, field.dataType)))
      .otherwise(F.lit(None))  # Handle potential null values in the struct
      .alias(field.name)
      for field in struct_fields
  ]

  # Transform the array of structs using withColumn
  return df.withColumn(
      array_column,
      F.array(F.struct(*cast_exprs))
  )


# Example usage (same as previous code)
