# temporal-tables-in-SQLite

A simple script to create a temporale table & triggers for an existing table in SQLite

## Usage

    printSQL.py db table

for example:

    printSQL.py ./SampleDB.db Test

Sample output:

```sql
/** ---- Create Table ----**/

CREATE TABLE _Test_history (
	    ID INTEGER,
	    Value TEXT,
	    Field3 NUMERIC,
        valid_from        TEXT,
        valid_to        TEXT
);
CREATE INDEX idx_Test_history_keys ON _Test_history ( ID, valid_from, valid_to );

/**--- Create Trigger ---**/

CREATE TRIGGER Test_insert_history
AFTER INSERT ON Test
BEGIN
    INSERT INTO _Test_history (ID, Value, Field3, valid_from, valid_to)
    VALUES (new.ID, new.Value, new.Field3, STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'), '9999-12-31 23:59:59.999');
END;

CREATE TRIGGER Test_update_history
AFTER UPDATE ON Test
WHEN ( old.ID is not new.ID or old.Value is not new.Value or old.Field3 is not new.Field3 )
BEGIN
        UPDATE _Test_history
        SET valid_to = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
        WHERE
                /* add Key colum where and */
        ID is old.ID and
                valid_to = '9999-12-31 23:59:59.999';
    INSERT INTO _Test_history ( ID, Value, Field3, valid_from, valid_to)
    VALUES( new.ID, new.Value, new.Field3, STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'), '9999-12-31 23:59:59.999');
END;

CREATE TRIGGER Test_delete_history
AFTER DELETE ON Test
BEGIN
    UPDATE _Test_history
        SET valid_to = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
        WHERE
                /* add Key colum where and */
        ID is old.ID and
                valid_to = '9999-12-31 23:59:59.999';
END;
```