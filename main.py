
import mysql.connector
from datetime import datetime
from faker import Faker
import time
from configdb import Schema, Table, CustomTable, Trigger
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)





class TriggerProcess():

    def __init__(self):
        self.date = self.getDateNow()
        self.conn = self.connect()
        self.cursor = self.conn.cursor()
        self.dbname = 'database_prueba'
        self.tablename = 'clientes'
        self.insert_rows = 400
        self.pkupdate = 1
        self.pkinsert = 999999


    class bcolors:
        GREEN = '\033[92m' 
        YELLOW = '\033[93m'  
        RED = '\033[91m'  
        RESET = '\033[0m' 


    def getDateNow(self):
        fecha_hora_actual = datetime.now()
        formato = "%Y-%m-%d %H:%M:%S.%f"
        date = fecha_hora_actual.strftime(formato)[:-3]
        return date
    
    def getData(self):
        faker = Faker()
        name = faker.name()
        email = faker.email()
        address = faker.address()
        return name, email, address

    def getInitialData(self, initalrows):
        dicts = []
        for rows in range(1, initalrows+1):
            name, email, address = self.getData()
            dict = {
                'id' : rows,
                'name' : name,
                'email' : email,
                'address' : address
            }
            dicts.append(dict)
        return dicts
    
    def getNewData(self, maxRows):
        dicts = []
        maxId = self.getMaxId()
        print(f"generando {str(maxRows)} datos...")
        for i in range(maxId+1, maxRows+1):
            print(f"Dato Nro: {i}")
            name, email, address = self.getData()
            dict = {
                'id' : i,
                'name' : name,
                'email' : email,
                'address' : address
            }
            dicts.append(dict)
        return dicts
    
    def getMaxId(self):
        self.cursor.execute(f"""SELECT MAX(id) FROM {self.dbname}.{self.tablename};""")
        max_id = self.cursor.fetchone()[0]
        return int(max_id)
    
    def delete(self, table, custom_table, trigger, schema):
        logger.info(self.bcolors.YELLOW+f"Delete trigger.." +self.bcolors.RESET)
        trigger.delete()

        logger.info(self.bcolors.YELLOW+f"Delete table.." +self.bcolors.RESET)
        table.delete()

        logger.info(self.bcolors.YELLOW+f"Delete custom table.." +self.bcolors.RESET)
        custom_table.delete()

        logger.info(self.bcolors.YELLOW+f"Delete db.." +self.bcolors.RESET)
        schema.delete()
    
    

    def connect(self):
        try:

            conn = mysql.connector.connect(
                    user='root', 
                    password='root', 
                    host='127.0.0.1', 
                    port="3306", 
                    database='db'
                )
            
        except Exception  as e:
            print(f"Error: {e}")
        return conn
    

    def run(self):
        logger.info("Iniciando el proceso..")
        try:
            schema = Schema(self.cursor, self.dbname, self.tablename)
            schema.create()
            table = Table(self.cursor, self.dbname, self.tablename)
            table.create()
            logger.info(self.bcolors.YELLOW+ f"Ok Schema y Table: {self.dbname}.{self.tablename}" +self.bcolors.RESET)
            time.sleep(1)
            
            self.cursor.execute(f"""USE {self.dbname};""")

            initialRows = 10
            data = self.getInitialData(initialRows)
            logger.info(self.bcolors.YELLOW+ f"Insertando {str(initialRows)} filas iniciales.."  +self.bcolors.RESET)
           
            for d in data:
                query = f"INSERT INTO {self.dbname}.{self.tablename} (id, name, email, address) VALUES (%s, %s, %s, %s);"
                valores = (d['id'], d['name'], d['email'], d['address'])
                self.cursor.execute(query, valores)

            self.cursor.execute(f"SELECT COUNT(*) FROM {self.dbname}.{self.tablename};")
            recuento_filas = self.cursor.fetchone()[0]
            logger.info(self.bcolors.YELLOW+ f"Total de filas iniciales de la tabla : {str(recuento_filas)}"   +self.bcolors.RESET)
           
            time.sleep(2)

            self.cursor.execute(f"SELECT * FROM {self.dbname}.{self.tablename} LIMIT 5;")
            total_rows = self.cursor.fetchall()
            logger.info(self.bcolors.YELLOW+ f"Primeras filas de la tabla.."  +self.bcolors.RESET)
            for fila in total_rows:
                time.sleep(0.5)
                print(fila)

            newData = self.getNewData(maxRows=int(self.insert_rows ))
            logger.info(self.bcolors.YELLOW+ f"Total de filas a ingresar : {len(newData)}"  +self.bcolors.RESET)
            for d in newData:
                query = f"INSERT INTO {self.dbname}.{self.tablename} (id, name, email, address) VALUES (%s, %s, %s, %s);"
                valores = (d['id'], d['name'], d['email'], d['address'])
                self.cursor.execute(query, valores)

            self.cursor.execute(f"SELECT COUNT(*) FROM {self.dbname}.{self.tablename};")
            recuento_filas_nuevas = self.cursor.fetchone()[0]
            logger.info(self.bcolors.YELLOW+ f"Total de filas finales de la tabla : {self.dbname}.{self.tablename}: {recuento_filas_nuevas}"  +self.bcolors.RESET)
            time.sleep(2)

            self.cursor.execute(f"SELECT * FROM {self.dbname}.{self.tablename} LIMIT 20;")
            total_new_rows = self.cursor.fetchall()
            logger.info(self.bcolors.YELLOW+ f"Primeras filas de los nuevos registros.."  +self.bcolors.RESET)
            
            for fila in total_new_rows:
                time.sleep(0.5)
                print(fila)

            print(self.bcolors.YELLOW+ f"Creando una tabla custom para: {self.dbname}.{self.tablename}" +self.bcolors.RESET)
            time.sleep(1)
            custom_table = CustomTable(self.cursor, self.dbname, self.tablename)
            custom_table.create()

            print(self.bcolors.YELLOW+ f"Creando un trigger para: {self.dbname}.{self.tablename}" +self.bcolors.RESET)
            time.sleep(4)
            trigger = Trigger(self.cursor, self.dbname, self.tablename)
            trigger.create()

            logger.info(self.bcolors.YELLOW+ f"Insertando un dato en la tabla principal: {self.dbname}.{self.tablename} con el PK: {self.pkinsert}"  +self.bcolors.RESET)
            time.sleep(2)
            name, email, address = self.getData()
            dict = {
                'id' : self.pkinsert,
                'name' : name,
                'email' : email,
                'address' : address
            }
            newData = [dict]
            logger.info(self.bcolors.YELLOW+ f"Total de filas a ingresar : {len(newData)}"  +self.bcolors.RESET)
            
            for d in newData:
                query = f"INSERT INTO {self.dbname}.{self.tablename} (id, name, email, address) VALUES (%s, %s, %s, %s);"
                valores = (d['id'], d['name'], d['email'], d['address'])
                self.cursor.execute(query, valores)


            logger.info(self.bcolors.YELLOW+ f"Actualizando un dato en la tabla principal: {self.dbname}.{self.tablename} con el PK: {self.pkupdate}"  +self.bcolors.RESET)
            
            time.sleep(2)
            query = f"UPDATE {self.dbname}.{self.tablename} SET name = %s WHERE id = %s;"
            valores = ('newname', self.pkupdate)
            self.cursor.execute(query, valores)

            logger.info(self.bcolors.YELLOW+ f"Verificando el PK del Insert ({self.pkinsert}) y del New Update ({self.pkupdate}) en la tabla custom: {self.dbname}.{self.tablename}_custom" +self.bcolors.RESET)
           
            self.cursor.execute(f"SELECT * FROM {self.dbname}.{self.tablename}_custom;")
            customtable = self.cursor.fetchall()
            logger.info(self.bcolors.YELLOW+f"Primeras filas de la custom table.." +self.bcolors.RESET)
           
            time.sleep(2)
            for fila in customtable:
                time.sleep(2)
                print(fila)
            logger.info(self.bcolors.GREEN+"Coinciden los PK actualizados e insertados...!"+self.bcolors.RESET)
           
            time.sleep(2)

            logger.info(self.bcolors.GREEN+"Inner Join entre la tabla custom y la principal."+self.bcolors.RESET)

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


            logger.info(self.bcolors.YELLOW+ f"Actualizando el mismo dato que antes en la tabla principal: {self.dbname}.{self.tablename} con el PK: {self.pkupdate}"  +self.bcolors.RESET)
            time.sleep(2)
            query = f"UPDATE {self.dbname}.{self.tablename} SET name = %s WHERE id = %s;"
            valores = ('newname_2', self.pkupdate)
            self.cursor.execute(query, valores)


            logger.info(self.bcolors.GREEN+ f"Ultimo Update para el PK {self.pkupdate} y el primer Insert para el PK {self.pkinsert}"  +self.bcolors.RESET)
            self.cursor.execute(queryinner)
            inner_rows = self.cursor.fetchall()
            for fila in inner_rows:
                time.sleep(0.2)
                print(fila)


            self.delete(table, custom_table, trigger, schema)


        except Exception  as e:
            print(f"Error: {e}")
            self.delete(table, custom_table, trigger, schema)

        self.conn.commit()

    

if __name__ == "__main__":
    obj = TriggerProcess()
    obj.run()
    print("Finish TriggerProcess")