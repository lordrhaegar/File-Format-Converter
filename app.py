import glob
import json
import pandas as pd
import os

def get_column_names(schema, ds_name, sorting_key="column_position"):
    tb_strct = sorted(schema[ds_name], key= lambda col: col[sorting_key])
    return [col["column_name"] for col in tb_strct]


def read_csv(file, ds_name, schema):
    col_names = get_column_names(schema, ds_name)
    return pd.read_csv(file, names=col_names)

def to_json(file_path, json_dir, ds_name, file_name, schema):
    df = read_csv(file_path, ds_name, schema)
    os.makedirs(f"./{json_dir}/{ds_name}", exist_ok=True)
    json_file = df.to_json(f'./{json_dir}/{ds_name}/{file_name}', orient='records', lines=True)


def file_converter(file_path, schema_path):
    trgt_dir = "./converted_json_versions"
    schema = json.load(open(schema_path, 'r'))
    os.makedirs(f"./{trgt_dir}", exist_ok=True)
    file_path_struct = file_path.split("/")
    ds_name = file_path_struct[-2]
    file_name = file_path_struct[-1]
    print(ds_name, file_name)
    to_json(file_path, trgt_dir, ds_name, file_name, schema)

if __name__ == '__main__':
    file_converter("path-to-file", "path-to-schemas.json")
    