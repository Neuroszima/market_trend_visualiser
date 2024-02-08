# following file should be used as the first one for setting up entire database
from db_functions.db_helpers import _connection_dict

import psycopg2


def import_db_structure_():
    with open('schema_dump.sql', 'r') as schema_sql:
        structure_description = schema_sql.readlines()
    structure_description = "".join(structure_description)

    with psycopg2.connect(**_connection_dict) as conn:
        cur: psycopg2.cursor = conn.cursor()
        cur.execute(structure_description)


if __name__ == '__main__':
    import_db_structure_()
