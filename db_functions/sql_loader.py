# following file should be used as the first one for setting up entire database
from os.path import abspath

from db_functions.db_helpers import _connection_dict

import psycopg2

build_schema_instructions_file_path = "\\".join(abspath(__file__).split("\\")[:-1] + ['schema_dump.sql'])
purge_schema_instructions_file_path = "\\".join(abspath(__file__).split("\\")[:-1] + ['schema_purge.sql'])


def __execute_instructions_from_file(instructions_file_path):
    with open(instructions_file_path, 'r') as schema_sql:
        instructions_for_db = schema_sql.readlines()
    instructions_for_db = "".join(instructions_for_db)

    with psycopg2.connect(**_connection_dict) as conn:
        cur: psycopg2.cursor = conn.cursor()
        cur.execute(instructions_for_db)


def import_db_structure_():
    """remove every element from the database, for testing purposes"""
    __execute_instructions_from_file(build_schema_instructions_file_path)


def purge_db_structure_():
    """remove every element from the database, for testing purposes"""
    __execute_instructions_from_file(purge_schema_instructions_file_path)


if __name__ == '__main__':
    purge_db_structure_()
    import_db_structure_()
