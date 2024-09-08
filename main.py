import mysql.connector
from datetime import datetime
from faker import Faker
import time
import logging
from typing import List, Dict, Any, Tuple
from dotenv import load_dotenv
import os


load_dotenv(override=True)
user = os.getenv("USER")
password = os.getenv("PASS")
host = os.getenv("HOST")
port= os.getenv("PORT")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




class TriggerProcess():

    def __init__(self,
                 host: str, 
                 user: str, 
                 password: str, 
                 port: str) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.date = self.getDateNow()
        self.conn = None
        self.cursor = None
        self.dbname = 'database_prueba'
        self.tablename = 'clientes'
        self.tablecustom = self.tablename + '_custom'
        self.triggername = self.tablename + '_trigger'
        self.triggerinsert = self.triggername + '_insert'
        self.triggerupdate = self.triggername + '_update'
        self.initialRows = 10 # rows iniciales a insertar
        self.insertRows = 30 # 30 rows mas que las rows ya existentes
        self.pkupdate = 1
        self.pkinsert = 999999
        self.faker = Faker()


    class bcolors:
        GREEN = '\033[92m' 
        YELLOW = '\033[93m'  
        RED = '\033[91m'  
        RESET = '\033[0m' 

    def createSchema(self) -> None:
        self.cursor.execute(f"""DROP SCHEMA IF EXISTS {self.dbname};""")
        self.cursor.execute(f"""CREATE SCHEMA IF NOT EXISTS {self.dbname};""")


    def createTable(self) -> None:
        self.cursor.execute(f"""DROP TABLE IF EXISTS {self.dbname}.{self.tablename};""")
        self.cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.dbname}.{self.tablename} (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                address VARCHAR(255)
            );"""
        )



    def createCustomTable(self) -> None:
        self.cursor.execute(f"""DROP TABLE IF EXISTS {self.dbname}.{self.tablecustom};""")
        self.cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.dbname}.{self.tablecustom} (
                pk INT,
                operacion VARCHAR(50),
                ultima_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP
            );"""
        )

    def createTriggers(self) -> None:
        self.cursor.execute(f"""DROP TRIGGER IF EXISTS {self.triggerinsert};""")
        self.cursor.execute(
            f"""CREATE TRIGGER {self.triggerinsert}
                AFTER INSERT ON {self.tablename}
                FOR EACH ROW
                BEGIN
                    INSERT INTO {self.tablecustom} (pk, operacion, ultima_actualizacion)
                    VALUES (NEW.id, 'insert', NOW());
                END"""
        )

        self.cursor.execute(f"""DROP TRIGGER IF EXISTS {self.triggerupdate};""")
        self.cursor.execute(
            f"""CREATE TRIGGER {self.triggerupdate}
                AFTER UPDATE ON {self.tablename}
                FOR EACH ROW
                BEGIN
                    INSERT INTO {self.tablecustom} (pk, operacion, ultima_actualizacion)
                    VALUES (NEW.id, 'update', NOW());
                END"""
        )


    def getDateNow(self) -> str:
        fecha_hora_actual = datetime.now()
        formato = "%Y-%m-%d %H:%M:%S.%f"
        date = fecha_hora_actual.strftime(formato)[:-3]
        return date
    
    def getData(self) -> tuple[str, str, str]:
        name = self.faker.name()
        email = self.faker.email()
        address = self.faker.address()
        return name, email, address

    def getInitialData(self, initialrows: int) -> List[Dict[str, str]]:
        dicts = []
        for rows in range(1, initialrows+1):
            name, email, address = self.getData()
            dict = {
                'id' : rows,
                'name' : name,
                'email' : email,
                'address' : address
            }
            dicts.append(dict)
        return dicts
    

    def getNewData(self, maxRows: int) -> List[Dict[str, str]]:
        dicts = []
        maxId = self.getMaxId()
        fromId = maxId+1
        toId = maxId+maxRows+1
        print(f"generando {str(maxRows)} datos...")
        for i in range(fromId, toId):
            name, email, address = self.getData()
            dict = {
                'id' : i,
                'name' : name,
                'email' : email,
                'address' : address
            }
            dicts.append(dict)
        return dicts
    
    def getMaxId(self) -> int:
        self.cursor.execute(f"""SELECT MAX(id) FROM {self.dbname}.{self.tablename};""")
        max_id = self.cursor.fetchone()[0]
        return int(max_id)
    
    def insertData(self, data: List[Dict[str, Any]]) -> None:
        for d in data:
            query = f"INSERT INTO {self.dbname}.{self.tablename} (id, name, email, address) VALUES (%s, %s, %s, %s);"
            valores = (d['id'], d['name'], d['email'], d['address'])
            self.cursor.execute(query, valores)
    
    def infoLogger(self, message):
        logger.info(self.bcolors.YELLOW+ message  +self.bcolors.RESET)

    def getSelect(self, table: str) -> List[Tuple[Any]]:
        self.cursor.execute(f"SELECT * FROM {table} LIMIT 5;")
        totalRows = self.cursor.fetchall()
        return totalRows
    
    def getCount(self, table: str) -> int:
        self.cursor.execute(f"SELECT COUNT(*) FROM {table};")
        rowsCount = self.cursor.fetchone()[0]
        return rowsCount

    def connect(self):
        try:
            self.conn = mysql.connector.connect(
                    user=self.user, 
                    password=self.password, 
                    host=self.host, 
                    port=str(self.port)
                )
            print(self.conn)
            self.cursor = self.conn.cursor()
        except Exception  as e:
            print(f"Error: {logger.error(self.bcolors.RED+ str(e)  +self.bcolors.RESET)}")


    def run(self):
        self.infoLogger(message="Iniciando el proceso..")
        try:
            self.connect()
            self.createSchema()
            self.createTable()
            self.infoLogger(message=f"Ok Schema y Table: {self.dbname}.{self.tablename}")
            time.sleep(1)
            
            self.cursor.execute(f"""USE {self.dbname};""")

            data = self.getInitialData(self.initialRows)
            self.infoLogger(message=f"Insertando {str(self.initialRows)} filas iniciales..")
            self.insertData(data)
            time.sleep(1)

            total_rows = self.getSelect(table=f"{self.dbname}.{self.tablename}")
            self.infoLogger(message=f"Primeras filas de la tabla..")
            for fila in total_rows:
                time.sleep(0.5)
                print(fila)

            newData = self.getNewData(maxRows=int(self.insertRows ))
            self.infoLogger(message=f"Total de filas a ingresar : {len(newData)}")
            self.insertData(newData)

            recuento_filas_nuevas = self.getCount(table=f"{self.dbname}.{self.tablename}")
            self.infoLogger(message=f"Total de filas finales de la tabla : {self.dbname}.{self.tablename}: {recuento_filas_nuevas}")
            time.sleep(2)

            total_new_rows = self.getSelect(table=f"{self.dbname}.{self.tablename}")
            self.infoLogger(message=f"Primeras filas de los nuevos registros..")
            for fila in total_new_rows:
                time.sleep(0.5)
                print(fila)

            self.infoLogger(message=f"Creando una tabla custom para: {self.dbname}.{self.tablename}")
            time.sleep(1)
            self.createCustomTable()
            self.infoLogger(message=f"Creando un trigger para: {self.dbname}.{self.tablename}")
            time.sleep(4)
            self.createTriggers()

            self.infoLogger(message=f"Insertando un dato en la tabla principal: {self.dbname}.{self.tablename} con el PK: {self.pkinsert}")
            time.sleep(2)

            name, email, address = self.getData()
            dict = {
                'id' : self.pkinsert,
                'name' : name,
                'email' : email,
                'address' : address
            }
            newData = [dict]
            self.infoLogger(message=f"Total de filas a ingresar : {len(newData)}")
            self.insertData(newData)

            self.infoLogger(message=f"Actualizando un dato en la tabla principal: {self.dbname}.{self.tablename} con el PK: {self.pkupdate}") 
            time.sleep(2)
            query = f"UPDATE {self.dbname}.{self.tablename} SET name = %s WHERE id = %s;"
            valores = ('newname', self.pkupdate)
            self.cursor.execute(query, valores)

            self.infoLogger(message=f"Verificando el PK del Insert ({self.pkinsert}) y del New Update ({self.pkupdate}) en la tabla custom: {self.dbname}.{self.tablename}_custom")
            
            customtable = self.getSelect(table=f"{self.dbname}.{self.tablename}_custom")
            self.infoLogger(message=f"Primeras filas de la custom table..")
            
           
            time.sleep(2)
            for fila in customtable:
                time.sleep(2)
                print(fila)
            self.infoLogger(message="Coinciden los PK actualizados e insertados...!")
            
           
            time.sleep(2)
            self.infoLogger(message="Inner Join entre la tabla custom y la principal.")
            
            queryinner = f"""SELECT c.*, cc.operacion, cc.ultima_actualizacion
                    FROM {self.dbname}.{self.tablename} c
                    INNER JOIN (
                        SELECT pk, operacion, MAX(ultima_actualizacion) AS ultima_actualizacion
                        FROM {self.dbname}.{self.tablename}_custom
                        GROUP BY pk, operacion
                    ) cc ON c.id = cc.pk;"""

            self.cursor.execute(queryinner)
            inner_rows = self.cursor.fetchall()
            for fila in inner_rows:
                time.sleep(0.2)
                print(fila)

            self.infoLogger(message=f"Actualizando el mismo dato que antes en la tabla principal: {self.dbname}.{self.tablename} con el PK: {self.pkupdate}")
            
            time.sleep(2)
            query = f"UPDATE {self.dbname}.{self.tablename} SET name = %s WHERE id = %s;"
            valores = ('newname_2', self.pkupdate)
            self.cursor.execute(query, valores)

            self.infoLogger(message=f"Ultimo Update para el PK {self.pkupdate} y el primer Insert para el PK {self.pkinsert}")
            
            self.cursor.execute(queryinner)
            inner_rows = self.cursor.fetchall()
            for fila in inner_rows:
                time.sleep(0.2)
                print(fila)


        except Exception  as e:
            print(f"Error: {e}")

        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    
if __name__ == "__main__":
    obj = TriggerProcess(host=host, user=user, password=password, port=port)
    obj.run()
    print("Finish TriggerProcess")