
class DatabaseObject():

    def __init__(self, cursor, dbname, tablename):
        self.cursor = cursor
        self.dbname = dbname
        self.tablename = tablename

    def create(self):
        pass

    def delete(self):
        pass


class Schema(DatabaseObject):

    def __init__(self, cursor, dbname, tablename):
        super().__init__(cursor, dbname, tablename)

    def create(self):
        self.cursor.execute(f"""CREATE SCHEMA IF NOT EXISTS {self.dbname};""")

    def delete(self):
        self.cursor.execute(f"""DROP SCHEMA IF EXISTS {self.dbname};""")


class Table(DatabaseObject):

    def __init__(self, cursor, dbname, tablename):
        super().__init__(cursor, dbname, tablename)

    def create(self):
        self.cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.dbname}.{self.tablename} (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                address VARCHAR(255)
            );"""
        )

    def delete(self):
        self.cursor.execute(f"""DROP TABLE IF EXISTS {self.dbname}.{self.tablename};""")


class CustomTable(DatabaseObject):

    def __init__(self, cursor, dbname, tablename):
        super().__init__(cursor, dbname, tablename)
        self.tablecustom = tablename + '_custom'

    def create(self):
        self.cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.dbname}.{self.tablecustom} (
                pk INT,
                operacion VARCHAR(50),
                ultima_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP
            );"""
        )

    def delete(self):
        self.cursor.execute(f"""DROP TABLE IF EXISTS {self.dbname}.{self.tablecustom};""")



class Trigger(DatabaseObject):

    def __init__(self, cursor, dbname, tablename):
        super().__init__(cursor, dbname, tablename)
        self.tablecustom = tablename + '_custom'
        self.triggername = tablename + '_trigger'
        self.triggerinsert = self.triggername + '_insert'
        self.triggerupdate = self.triggername + '_update'

    def create(self):
        self.cursor.execute(
            f"""CREATE TRIGGER {self.triggerinsert}
                AFTER INSERT ON {self.tablename}
                FOR EACH ROW
                BEGIN
                    INSERT INTO {self.tablecustom} (pk, operacion, ultima_actualizacion)
                    VALUES (NEW.id, 'insert', NOW());
                END"""
        )

        self.cursor.execute(
            f"""CREATE TRIGGER {self.triggerupdate}
                AFTER UPDATE ON {self.tablename}
                FOR EACH ROW
                BEGIN
                    INSERT INTO {self.tablecustom} (pk, operacion, ultima_actualizacion)
                    VALUES (NEW.id, 'update', NOW());
                END"""
        )

    def delete(self):
        self.cursor.execute(f"""DROP TRIGGER IF EXISTS {self.triggerinsert};""")

        self.cursor.execute(f"""DROP TRIGGER IF EXISTS {self.triggerupdate};""")