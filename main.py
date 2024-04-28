import mysql.connector
from datetime import datetime
from faker import Faker
import time
from configdb import Schema, Table, CustomTable, Trigger
import logging
from typing import List, Dict, Any, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




class TriggerProcess():

    def __init__(self):
        self.date = self.getDateNow()
        self.conn = self.connect()
        self.cursor = self.conn.cursor()
        self.dbname = 'database_prueba'
        self.tablename = 'clientes'
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
            print('Conectando..')
            conn = mysql.connector.connect(
                    user='root', 
                    password='root', 
                    host='127.0.0.1', 
                    port="3306"
                )

        except Exception  as e:
            print(f"Error: {e}")
        return conn


    def run(self):
        self.infoLogger(message="Iniciando el proceso..")
        try:
            schema = Schema(self.cursor, self.dbname, self.tablename)
            schema.create()
            
            table = Table(self.cursor, self.dbname, self.tablename)
            table.create()

            self.infoLogger(message=f"Ok Schema y Table: {self.dbname}.{self.tablename}")
            time.sleep(1)
            
            self.cursor.execute(f"""USE {self.dbname};""")

            data = self.getInitialData(self.initialRows)
            self.infoLogger(message=f"Insertando {str(self.initialRows)} filas iniciales..")
            self.insertData(data)
            time.sleep(2)

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
            customTable = CustomTable(self.cursor, self.dbname, self.tablename)
            customTable.create()
            self.infoLogger(message=f"Creando un trigger para: {self.dbname}.{self.tablename}")
            time.sleep(4)
            trigger = Trigger(self.cursor, self.dbname, self.tablename)
            trigger.create()

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

            self.infoLogger(message="Delete..")
            trigger.delete()
            table.delete()
            customTable.delete()
            schema.delete()


        except Exception  as e:
            print(f"Error: {e}")
            try:
                trigger.delete()
            except:
                pass
            try:
                table.delete()
            except:
                pass
            try:
                customTable.delete()
            except:
                pass
            try:
                schema.delete()
            except:
                pass

        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    
if __name__ == "__main__":
    obj = TriggerProcess()
    obj.run()
    print("Finish TriggerProcess")