# encoding: utf-8
import os

import pyodbc


def create_dirs_if_not_exist(path):
    """
    check if all directories in path
    :param path:
    :return:
    """
    if not os.path.exists(path):
        os.makedirs(path)


def get_package_id_dir_map(cursor):
    """
    return a map of {"packageId": "dir"}
    :param cursor:
    :return:
    """
    select_from_query_group = """SELECT [PackageId]
          ,[Name]
          ,[FileName]
          ,[isReadOnly]
          ,[Severity]
          ,[isEncrypted]
          ,[Description]
          ,[Language]
          ,[LanguageName]
          ,[PackageType]
          ,[PackageTypeName]
          ,[Project_Id]
          ,[is_deprecated]
          ,[Owning_Team]
      FROM [CxDB].[dbo].[QueryGroup];"""

    rows = cursor.execute(select_from_query_group)
    package_id_dir_dict = dict()
    for group_row in rows:
        language_name = group_row.LanguageName
        package_type_name = group_row.PackageTypeName
        package_name = group_row.Name
        package_id = group_row.PackageId

        path = base_dir + "\\" + language_name + "\\" + package_type_name + "\\" + package_name
        package_id_dir_dict[package_id] = path
    return package_id_dir_dict


def store_queries(package_id_dir_map, cursor):
    select_from_query = """SELECT [QueryId]
          ,[PackageId]
          ,[Name]
          ,[Source]
          ,[DraftSource]
          ,[Cwe]
          ,[Comments]
          ,[Severity]
          ,[isExecutable]
          ,[isEncrypted]
          ,[is_deprecated]
          ,[IsCheckOut]
          ,[UpdateTime]
          ,[CurrentUserName]
          ,[IsCompiled]
          ,[CxDescriptionID]
          ,[EngineMetadata]
      FROM [CxDB].[dbo].[Query]"""

    rows = cursor.execute(select_from_query)
    for query_row in rows:
        package_id = query_row.PackageId
        query_name = query_row.Name
        query_source = query_row.Source

        directory = package_id_dir_map.get(package_id)
        if not directory:
            continue
        create_dirs_if_not_exist(directory)
        file_name = directory + "\\" + query_name + ".txt"
        with open(file_name, "a") as file:
            file.write(query_source)


if __name__ == '__main__':
    base_dir = "C:\\Data\\CxQueries"

    with pyodbc.connect("",
                        SERVER="HAPPYY-LAPTOP\\SQLEXPRESS",
                        DRIVER='{ODBC Driver 17 for SQL Server}',
                        DATABASE='CxDB',
                        Trusted_Connection='yes') as connection:
        cur = connection.cursor()
        id_dir_map = get_package_id_dir_map(cur)
        store_queries(id_dir_map, cur)
