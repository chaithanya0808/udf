You can avoid repeating the code by creating a function to handle the common part of appending to jsonb_dict and jsonb_dict_array. Here's how you can modify the code:

def add_to_jsonb_dict(json_dict, db_json_field, fcem_attribute, udf_attribute):
    json_dict.setdefault(db_json_field, []).append(
        (
            fcem_attribute,
            udf_attribute,
        )
    )

#creating above dictionaries
for row in dict_reader:
    udf_attribute = row["udf_table_name"] + "|" + row["udf_key"]
    pivot_values.append(udf_attribute)
    # if the column is part of a structure column, add to jsonb_dict_array
    if row["fcem_db_json_field"] and row["is_array"] == 'Y':
        add_to_jsonb_dict(jsonb_dict_array, row["fcem_db_json_field"], row["fcem_attribute"], udf_attribute)
    # for any case, add to jsonb_dict
    if row["fcem_db_json_field"]:
        add_to_jsonb_dict(jsonb_dict, row["fcem_db_json_field"], row["fcem_attribute"], udf_attribute)
    else:
        first_class_dict[row["fcem_attribute"]] = udf_attribute


To reduce duplicate code, you can combine the logic for creating the struct columns into a reusable function. Here's the modified code:


def create_struct_columns(col_list):
    return [
        F.struct(
            *[
                udf_pivot[tuple_element[1]].alias(tuple_element[0])
                for tuple_element in col_list
            ]
        ).alias(alias_col)
        for alias_col, col_list in col_list.items()
    ]

# Customize the code
udf_pivot_model = udf_pivot.select(
    *select_key_cols,
    *[udf_pivot[udf_val].alias(alias_col) for alias_col, udf_val in first_class_dict.items()],
    *create_struct_columns(jsonb_dict),
    *create_struct_columns(jsonb_dict_array)
)



To reduce duplicate code, you can define a function to generate the aggregation expressions for both min_by and collect_list. Here's how you can modify the code:

def generate_aggregations(columns, agg_func):
    return [
        agg_func(col_name, "UDF_ROW").alias(col_name)
        for col_name in columns
    ]

# Customize the code
udf_pivot_model.groupBy("CASE_RK", "RECORD_START_TS").agg(
    *generate_aggregations(first_class_dict, F.min_by),
    *generate_aggregations(jsonb_dict, F.min_by),
    *generate_aggregations(first_class_dict, F.collect_list)
)



