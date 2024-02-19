#!/usr/bin/python3

import sqlite3
import re

reserved_words = set(
    ['abort', 'action', 'add', 'after', 'all', 'alter', 'analyze', 
     'and', 'as', 'asc', 'attach', 'autoincrement', 'before', 'begin', 
     'between', 'by', 'cascade', 'case', 'cast', 'check', 'collate', 'column', 
     'commit', 'conflict', 'constraint', 'create', 'cross', 'current_date', 
     'current_time', 'current_timestamp', 'database', 'default', 'deferrable', 
     'deferred', 'delete', 'desc', 'detach', 'distinct', 'drop', 'each', 
     'else', 'end', 'escape', 'except', 'exclusive', 'exists', 'explain', 
     'fail', 'for', 'foreign', 'from', 'full', 'glob', 'group', 'having', 'if',
     'ignore', 'immediate', 'in', 'index', 'indexed', 'initially', 'inner', 
     'insert', 'instead', 'intersect', 'into', 'is', 'isnull', 'join', 'key', 
     'left', 'like', 'limit', 'match', 'natural', 'no', 'not', 'notnull', 
     'null', 'of', 'offset', 'on', 'or', 'order', 'outer', 'plan', 'pragma', 
     'primary', 'query', 'raise', 'recursive', 'references', 'regexp', 
     'reindex', 'release', 'rename', 'replace', 'restrict', 'right', 
     'rollback', 'row', 'savepoint', 'select', 'set', 'table', 'temp', 
     'temporary', 'then', 'to', 'transaction', 'trigger', 'union', 'unique', 
     'update', 'using', 'vacuum', 'values', 'view', 'virtual', 'when', 
     'where', 'with', 'without']
)
# From https://www.sqlite.org/lang_keywords.html

_normalWordsRegex = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
def escape_sqlite(s):
    """ just some low effort escaping - 
        works for this purpose but is not secure!"""
    if _normalWordsRegex.match(s) and (s.lower() not in reserved_words):
        return s
    else:
        return f"[{s}]"


# Analyse functions
def table_columns_and_types(db, table: str) -> list:
    """ Get a list of tuple (name, type) of table columns """
    cursor = db.cursor()
    rows = cursor.execute("select name, type from pragma_table_info( :table_name)", {'table_name': table})
    columns_and_types = []
    for row in rows.fetchall():
        columns_and_types.append((row[0], row[1]))
    return columns_and_types


def table_key_columns(db, table: str) -> list:
    """ Get a list of tuple (name, type) of tables key-columns """
    cursor = db.cursor()
    rows = cursor.execute(
            """SELECT name, type from pragma_table_info( :table_name) as l 
                WHERE l.pk = 1 """, {'table_name': table})
    keyColumns = []
    for row in rows.fetchall():
        keyColumns.append((row[0], row[1]))
    return keyColumns


# create Table
def history_table_sql(table:str, columns_and_types:list, key_columns:list) -> str:
    """Return SQL statement to create history table for a table and its columns."""
    column_names = ",\n".join(
        "    {name} {type}".format(name=escape_sqlite(name), type=type)
        for name, type in columns_and_types
    )
    key_columns_str = ", ".join([escape_sqlite(str(i)) for i in key_columns])
    return """
CREATE TABLE _{table}_history (
{column_names},
        valid_from        TEXT,
        valid_to        TEXT
);
CREATE INDEX idx_{table}_history_keys ON _{table}_history ( {key_columns}, valid_from, valid_to );
""".format(
        table=table, column_names=column_names, key_columns=key_columns_str
    )


# create Trigger
def triggers_sql(table:str, columns:list, key_columns:list) -> str:
    """Return SQL for triggers for a table and its columns."""
    column_names_str = ", ".join(escape_sqlite(column) for column in columns)
    new_column_values_str = ", ".join("new." + escape_sqlite(column) for column in columns)
    insert_trigger = """
CREATE TRIGGER {table}_insert_history
AFTER INSERT ON {table}
BEGIN
    INSERT INTO _{table}_history ({column_names_str}, valid_from, valid_to)
    VALUES ({new_column_values_str}, STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'), '9999-12-31 23:59:59.999');
END;
""".format(
        table=table,
        column_names_str=column_names_str,
        new_column_values_str=new_column_values_str
    )
    update_columns = []
    for column in columns:
        update_columns.append(
            """old.{column} is not new.{column}""".format(
                column=escape_sqlite(column)
            )
        )
    update_columns_sql = " or ".join(update_columns)
    keyColumn_where = []
    for column in key_columns:
        keyColumn_where.append(
            """{column} is old.{column}""".format(
                column=escape_sqlite(column)
                )
            )
    keyColumn_where_sql = " and ".join(keyColumn_where)
    update_trigger = """
CREATE TRIGGER {table}_update_history
AFTER UPDATE ON {table}
WHEN ( {update_columns_sql} )
BEGIN
        UPDATE _{table}_history
        SET valid_to = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
        WHERE
                /* add Key colum where and */
        {keyColumn_where_sql} and
                valid_to = '9999-12-31 23:59:59.999';
    INSERT INTO _{table}_history ( {column_names}, valid_from, valid_to)
    VALUES( {new_column_values_str}, STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'), '9999-12-31 23:59:59.999');
END;
""".format(
        table=table,
        column_names=column_names_str,
        update_columns_sql=update_columns_sql,
        keyColumn_where_sql=keyColumn_where_sql,
        new_column_values_str=new_column_values_str
    )
    delete_trigger = """
CREATE TRIGGER {table}_delete_history
AFTER DELETE ON {table}
BEGIN
    UPDATE _{table}_history
        SET valid_to = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
        WHERE
                /* add Key colum where and */
        {keyColumn_where_sql} and
                valid_to = '9999-12-31 23:59:59.999';
END;
""".format(
        table=table,
        keyColumn_where_sql=keyColumn_where_sql
    )
    return insert_trigger + update_trigger + delete_trigger


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('db', help="SQLite-DB File")
    parser.add_argument('table', help="Table-Name for which a temoral table & trigger should be printed")
    args = parser.parse_args()

    db = sqlite3.connect(args.db)
    columnsAndTypes = table_columns_and_types(db, args.table)
    keyColumns = table_key_columns(db, args.table)
    historySql = history_table_sql(args.table, columnsAndTypes, [name for name, type in keyColumns])
    #print(keyColumns)
    print("/** ---- Create Table ----**/")
    print(historySql)
    print("/**--- Create Trigger ---**/")
    trigger = triggers_sql(args.table, [name for name, type in columnsAndTypes], [name for name, type in keyColumns])
    print(trigger)