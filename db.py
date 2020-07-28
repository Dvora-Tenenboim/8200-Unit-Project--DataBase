from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type
import os
import pathlib
import json
import shutil
from bidict import bidict
import db_api
from dataclasses_json import dataclass_json
from collections import defaultdict
from os import path

DB_ROOT = Path('db_files')


@dataclass_json
@dataclass
class DBField(db_api.DBField):
    name: str
    type: Type

    def __init__(self, name: str, type: Type):
        self.name = name
        self.type = type


def criteria_is_met(key_value, operator, value):
    if operator == "=":
        operator="=="
    regular_str = str(key_value) + operator + str(value)
    return eval(regular_str)


@dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any

    def __init__(self, field_name: str, operator: str, value: Any):
        self.field_name = field_name
        self.operator = operator
        self.value = value

    def is_met(self, field_value):
        regular_str = str(field_value) + self.operator + str(self.value)
        return eval(regular_str)


def get_dict_from_file(field_file_path):
    with open(field_file_path, "a+") as field_file:
        pass
    try:
        with open(field_file_path) as field_file:
            _dict = json.load(field_file)
    except ValueError:
        _dict = {}
    return _dict


def init_dict_field_names_field_ids(fields: List[DBField], key_field_name):
    dict_field_names_field_ids = {}
    for field_id, field in enumerate(fields):
        if not key_field_name == field.name:
            dict_field_names_field_ids[field.name] = field_id
    return bidict(dict_field_names_field_ids)


def get_field_file_path(table_dir_name, field_name) -> str:
    return os.path.join(table_dir_name, field_name + ".json")


def is_exist_key(map_keys_fields_ids, key):
    return map_keys_fields_ids.get(str(key))


def key_field_is_exist(map_keys_fields_ids: dict, key_name: str) -> bool:
    return map_keys_fields_ids.get(key_name)


def get_list_files_ids(dict_field_names_field_ids: bidict, values: Dict[str, Any]):
    return [dict_field_names_field_ids[field_name] for field_name in values]


def insert_key_field_ids(table_dir_path: str, key_field_name: str, curr_key: int, values: Dict[str, Any],
                         dict_field_ids_field_names) -> None:
    keys_file_path = get_field_file_path(table_dir_path, key_field_name)
    map_keys_fields_ids = get_dict_from_file(keys_file_path)
    if not is_exist_key(map_keys_fields_ids, curr_key):
        map_keys_fields_ids[curr_key] = get_list_files_ids(dict_field_ids_field_names, values)
    else:
        raise ValueError("key is exist already")  # TODO more specific
    with open(keys_file_path, 'w') as keys_file:
        json.dump(map_keys_fields_ids, keys_file, default=str)


def get_and_delete_key_field_ids(table_dir_path, key_field_name, key_to_del):
    keys_file_path = get_field_file_path(table_dir_path, key_field_name)
    map_keys_fields_ids = get_dict_from_file(keys_file_path)
    field_ids = []
    flag = 0
    for key in map_keys_fields_ids.keys():
        if key == str(key_to_del):
            field_ids = map_keys_fields_ids[key]
            del map_keys_fields_ids[key]
            flag = 1
            break
    with open(keys_file_path, 'w') as keys_file:
        json.dump(map_keys_fields_ids, keys_file, default=str)
    if flag:
        return field_ids
    raise ValueError("key is not appear")


def get_key_field_ids(table_dir_path, key_field_name, key):
    keys_file_path = get_field_file_path(table_dir_path, key_field_name)
    map_keys_fields_ids = get_dict_from_file(keys_file_path)
    for curr_key in map_keys_fields_ids.keys():
        if curr_key == str(key):
            return map_keys_fields_ids[curr_key]
    raise ValueError("key is not appear")


def get_field_names(dict_field_names_field_ids, field_ids):
    return [dict_field_names_field_ids.inverse[field_id] for field_id in field_ids]


def update_dict_field_names_field_ids(dict_field_names_field_ids, values):
    for field_name in values:
        if not dict_field_names_field_ids.get(field_name):
            dict_field_names_field_ids.forceput(field_name, len(dict_field_names_field_ids) + 1)


def get_map_key_num_of_true_criteria(table_dir_path, key_field_name, criteria: List[SelectionCriteria]):
    map_key_num_of_true_criteria = defaultdict(int)
    map_field_name_criteria_list = defaultdict(list)
    for selection_criteria in criteria:
        map_field_name_criteria_list[selection_criteria.field_name].append(selection_criteria)
    for field_name, criteria_list in map_field_name_criteria_list.items():
        field_file_path = get_field_file_path(table_dir_path, field_name)
        map_key_value_field_value = get_dict_from_file(field_file_path)
        if field_name == key_field_name:
            for key_value in map_key_value_field_value.keys():
                for selection_criteria in criteria_list:
                    print(criteria_list)
                    print(selection_criteria)
                    # if selection_criteria.is_met(key_value):
                    if criteria_is_met(key_value, selection_criteria.operator, selection_criteria.value):
                        map_key_num_of_true_criteria[key_value] += 1
        else:
            for key_value, field_value in map_key_value_field_value.items():
                for selection_criteria in criteria_list:
                    if criteria_is_met(field_value, selection_criteria.operator, selection_criteria.value):
                        map_key_num_of_true_criteria[key_value] += 1
    return map_key_num_of_true_criteria


def get_table_data_path(table_dir_path):
    return os.path.join(table_dir_path, "table_data", "table_data.json")


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str
    table_dir_path: str
    dict_field_names_field_ids: bidict
    counter: int

    def __init__(self, name: str, fields: List[DBField], key_field_name: str, table_dir_path: str, counter: int = 0,
                 dict_field_names_field_ids: dict = None):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name
        self.table_dir_path = table_dir_path
        self.counter = counter
        self.dict_field_names_field_ids = init_dict_field_names_field_ids(fields,
                                                                          key_field_name) if dict_field_names_field_ids is None else dict_field_names_field_ids
        self.update_table_data_file()

    def count(self) -> int:
        return self.counter

    def insert_record(self, values: Dict[str, Any]) -> None:
        if not key_field_is_exist(values, self.key_field_name):
            raise ValueError("key is exist already")  # TODO more specific
        curr_key = values[self.key_field_name]
        del values[self.key_field_name]
        update_dict_field_names_field_ids(self.dict_field_names_field_ids, values)
        insert_key_field_ids(self.table_dir_path, self.key_field_name, curr_key, values,
                             self.dict_field_names_field_ids)
        for field_name, field_value in values.items():
            field_file_path = get_field_file_path(self.table_dir_path, field_name)
            map_key_value_field_value = get_dict_from_file(field_file_path)
            map_key_value_field_value[curr_key] = field_value
            with open(field_file_path, "w") as field_file:
                json.dump(map_key_value_field_value, field_file, default=str)
        self.counter += 1
        self.update_table_data_file()

    def delete_record(self, key: Any) -> None:
        field_ids = get_and_delete_key_field_ids(self.table_dir_path, self.key_field_name, key)
        field_names = get_field_names(self.dict_field_names_field_ids, field_ids)
        for field_name in field_names:
            field_file_path = get_field_file_path(self.table_dir_path, field_name)
            map_key_value_field_value = get_dict_from_file(field_file_path)
            del map_key_value_field_value[str(key)]
            with open(field_file_path, "w") as field_file:
                json.dump(map_key_value_field_value, field_file, default=str)
        self.counter -= 1
        self.update_table_data_file()

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        map_key_num_of_true_criteria = get_map_key_num_of_true_criteria(self.table_dir_path,
                                                                        self.key_field_name, criteria)
        expected_criteria_num = len(criteria)
        for key_value, num_of_true_criteria in map_key_num_of_true_criteria.items():
            if num_of_true_criteria == expected_criteria_num:
                self.delete_record(key_value)

    def delete_DBTable(self):
        shutil.rmtree(self.table_dir_path)

    def get_record(self, key: Any) -> Dict[str, Any]:
        field_ids = get_key_field_ids(self.table_dir_path, self.key_field_name, key)
        field_names = get_field_names(self.dict_field_names_field_ids, field_ids)
        requested_record = {self.key_field_name: key}
        for field_name in field_names:
            field_file_path = get_field_file_path(self.table_dir_path, field_name)
            map_key_value_field_value = get_dict_from_file(field_file_path)
            requested_record[field_name] = map_key_value_field_value[str(key)]
        return requested_record

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        self.delete_record(key)
        values[self.key_field_name] = key
        self.insert_record(values)
        self.update_table_data_file()

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        map_key_num_of_true_criteria = get_map_key_num_of_true_criteria(self.table_dir_path,
                                                                        self.key_field_name, criteria)
        expected_criteria_num = len(criteria)
        requested_table = []
        for key_value, num_of_true_criteria in map_key_num_of_true_criteria.items():
            if num_of_true_criteria == expected_criteria_num:
                requested_table.append(self.get_record(key_value))
        return requested_table

    def create_index(self, field_to_index: str) -> None:

        raise NotImplementedError

    def update_table_data_file(self):
        dict_table_data = {"fields": self.fields, "key_field_name": self.key_field_name,
                           "table_dir_path": self.table_dir_path,
                           "counter": self.counter, "dict_field_names_field_ids": self.dict_field_names_field_ids}

        with open(get_table_data_path(self.table_dir_path), "w") as table_data_file:
            json.dump(dict_table_data, table_data_file, default=str)


def init_table_dir(table_name: str):
    # parent_dir = pathlib.Path().absolute()
    # table_dir_path = os.path.join(parent_dir, f"{DB_ROOT}", name)
    table_dir_path = get_table_dir_path(table_name)
    try:
        os.mkdir(table_dir_path)
    except OSError as error:
        print(f"{error} you have already created a table named {table_name}")
    init_table_data_file(table_dir_path)


def init_table_data_file(table_dir_path: str):
    os.mkdir(os.path.join(table_dir_path, "table_data"))
    with open(get_table_data_path(table_dir_path), "a+", encoding="utf8") as table_data_file:
        pass


def get_table_dir_path(table_name: str):
    return f'{DB_ROOT}\\{table_name}'


def get_table_data(table_path):
    table_data_path = get_table_data_path(table_path)
    return get_dict_from_file(table_data_path)


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    # Put here any instance information needed to support the API

    path: str
    map_table_name_table_path: dict
    map_table_name_table_DBTable_object: dict

    def __init__(self):
        self.path = f'{DB_ROOT}\\tables.json'
        self.map_table_name_table_path = {}
        self.map_table_name_table_DBTable_object = {}
        self.load_tables()

    def load_tables(self):
        if path.exists(self.path):
            with open(self.path, encoding="utf8") as the_file:
                tmp = json.load(the_file)
                if len(tmp) > 0:
                    self.map_table_name_table_path = dict(tmp)
        map_table_name_table_path = get_dict_from_file(self.path)
        for table_name, table_path in map_table_name_table_path.items():
            map_table_name_table_data = get_table_data(table_path)
            if map_table_name_table_data:
                self.map_table_name_table_DBTable_object[table_name] = DBTable(table_name,
                                                                               map_table_name_table_data["fields"],
                                                                               map_table_name_table_data[
                                                                                   "key_field_name"],
                                                                               table_path,
                                                                               map_table_name_table_data["counter"],
                                                                               map_table_name_table_data[
                                                                                   "dict_field_names_field_ids"])

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        init_table_dir(table_name)
        table_dir_path = get_table_dir_path(table_name)

        new_table = DBTable(table_name, fields, key_field_name, table_dir_path)
        self.map_table_name_table_DBTable_object[table_name] = new_table
        map_table_name_table_path = get_dict_from_file(self.path)
        map_table_name_table_path[table_name] = table_dir_path
        with open(self.path, "w", encoding="utf8") as tables_file:
            json.dump(map_table_name_table_path, tables_file)
        return new_table

    def num_tables(self) -> int:
        return len(self.map_table_name_table_DBTable_object)

    def get_table(self, table_name: str) -> DBTable:
        return self.map_table_name_table_DBTable_object[table_name]

    def delete_table(self, table_name: str) -> None:
        self.map_table_name_table_DBTable_object[table_name].delete_DBTable()
        del self.map_table_name_table_DBTable_object[table_name]
        del self.map_table_name_table_path[table_name]
        map_table_name_table_path = get_dict_from_file(self.path)
        del map_table_name_table_path[table_name]
        with open(self.path, "w", encoding="utf8") as tables_file:
            json.dump(map_table_name_table_path, tables_file)

    def get_tables_names(self) -> List[Any]:
        return [table_name for table_name in self.map_table_name_table_DBTable_object.keys()]

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        # if len(tables)==0:
        #     return []
        # list_dbtable = [self.get_table(table_name) for table_name in tables]
        # comparison_table = list_dbtable[0].query_table(fields_and_values_list[0])
        # for table_num, dbtable in enumerate(list_dbtable[1:], 1):
        #     curr_table = dbtable.query_table(fields_and_values_list[table_num])
        pass


# _id = DBField("id", int)
# age = DBField("age", int)
# _list = [_id, age]
# db = DataBase()
# t1 = db.create_table("teacher", _list, "id")
# t1.insert_record({"id": 5, "age": 7, "height": 30})
# t1.insert_record({"id": 6, "age": 10, "height": 25})
# t1.insert_record({"id": 8, "age": 20, "height": 30})
# t1.delete_record(6)
# print(t1.get_record(5))
# a = SelectionCriteria("id", ">", 7)
# b = SelectionCriteria("height", "<", 50)
# c = SelectionCriteria("height", ">", 20)
# criteria = [a, b, c]
# print(t1.query_table(criteria))
# t1.delete_records(criteria)

# data = mpu.io.read('example.json')
# pip install mpu
# mpu.io.write('example.json', data)
s = SelectionCriteria("F", "==", 1000020)
s.is_met(80)
