
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime
from faker import Faker
import time
from configdb import Schema, Table, CustomTable, Trigger





class TriggerProcess():

    def __init__(self):
        self.date = self.getDateNow()
        self.conn = self.connect()
        self.cursor = self.conn.cursor()
        self.dbname = 'database_prueba'
        self.tablename = 'clientes'
        self.insert_rows = 100
        self.pkupdate = 1
        self.pkinsert = 999999


    class bcolors:
        OK = '\033[92m'  # GREEN
        WARNING = '\033[93m'  # YELLOW
        RED = '\033[91m'  # RED
        RESET = '\033[0m'  # RESET COLOR


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
    
    

    def connect(self):
        try:
            print('conectando..')
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
        print(self.bcolors.OK+ "Iniciando el proceso.." +self.bcolors.RESET)
        
        try:

            schema = Schema(self.cursor, self.dbname, self.tablename)
            schema.create()
            table = Table(self.cursor, self.dbname, self.tablename)
            table.create()
            print(self.bcolors.OK+ f"Ok Schema y Table: {self.dbname}.{self.tablename}" +self.bcolors.RESET)
            time.sleep(1)
            
            self.cursor.execute(f"""USE {self.dbname};""")

            initialRows = 10
            data = self.getInitialData(initialRows)
            print(self.bcolors.OK+ f"Insertando {str(initialRows)} filas iniciales.." +self.bcolors.RESET)
            for d in data:
                
                query = f"INSERT INTO {self.dbname}.{self.tablename} (id, name, email, address) VALUES (%s, %s, %s, %s);"
                valores = (d['id'], d['name'], d['email'], d['address'])
                self.cursor.execute(query, valores)

            self.cursor.execute(f"SELECT COUNT(*) FROM {self.dbname}.{self.tablename};")
            recuento_filas = self.cursor.fetchone()[0]
            print(f"Total de filas iniciales de la tabla : {self.dbname}.{self.tablename}", str(recuento_filas))
            time.sleep(2)

            
            self.cursor.execute(f"SELECT * FROM {self.dbname}.{self.tablename} LIMIT 5;")
            total_rows = self.cursor.fetchall()
            print(self.bcolors.OK+ f"Primeras filas de la tabla.." +self.bcolors.RESET)
            for fila in total_rows:
                time.sleep(0.5)
                print(fila)

            newData = self.getNewData(maxRows=int(self.insert_rows ))
            print(self.bcolors.OK+ f"Total de filas a ingresar : {len(newData)}" +self.bcolors.RESET)
            for d in newData:
                query = f"INSERT INTO {self.dbname}.{self.tablename} (id, name, email, address) VALUES (%s, %s, %s, %s);"
                valores = (d['id'], d['name'], d['email'], d['address'])
                self.cursor.execute(query, valores)

            self.cursor.execute(f"SELECT COUNT(*) FROM {self.dbname}.{self.tablename};")
            recuento_filas_nuevas = self.cursor.fetchone()[0]
            print(f"Total de filas finales de la tabla : {self.dbname}.{self.tablename}: ", str(recuento_filas_nuevas))
            time.sleep(2)


            print("Primeras 20 filas de la tabla:")
            self.cursor.execute(f"SELECT * FROM {self.dbname}.{self.tablename} LIMIT 20;")
            total_new_rows = self.cursor.fetchall()
            print(self.bcolors.OK+ f"Primeras filas de los nuevos registros.." +self.bcolors.RESET)
            for fila in total_new_rows:
                time.sleep(0.5)
                print(fila)

            print(self.bcolors.WARNING+ f"Creando una tabla custom para: {self.dbname}.{self.tablename}" +self.bcolors.RESET)
            time.sleep(1)
            custom_table = CustomTable(self.cursor, self.dbname, self.tablename)
            custom_table.create()

            print(self.bcolors.WARNING+ f"Creando un trigger para: {self.dbname}.{self.tablename}" +self.bcolors.RESET)
            time.sleep(4)
            trigger = Trigger(self.cursor, self.dbname, self.tablename)
            trigger.create()

            print(f"Insertando un dato en la tabla principal: {self.dbname}.{self.tablename} con el PK: {self.pkinsert}")
            time.sleep(2)
            name, email, address = self.getData()
            dict = {
                'id' : self.pkinsert,
                'name' : name,
                'email' : email,
                'address' : address
            }
            newData = [dict]
            print(self.bcolors.OK+ f"Total de filas a ingresar : {len(newData)}" +self.bcolors.RESET)
            for d in newData:
                query = f"INSERT INTO {self.dbname}.{self.tablename} (id, name, email, address) VALUES (%s, %s, %s, %s);"
                valores = (d['id'], d['name'], d['email'], d['address'])
                self.cursor.execute(query, valores)


            print(f"Actualizando un dato en la tabla principal: {self.dbname}.{self.tablename} con el PK: {self.pkupdate}")
            time.sleep(2)
            query = f"UPDATE {self.dbname}.{self.tablename} SET name = %s WHERE id = %s;"
            valores = ('newname', self.pkupdate)
            self.cursor.execute(query, valores)

            print(self.bcolors.RED+ f"Verificando el PK del Insert ({self.pkinsert}) y del New Update ({self.pkupdate}) en la tabla custom: {self.dbname}.{self.tablename}_custom" +self.bcolors.RESET)

            self.cursor.execute(f"SELECT * FROM {self.dbname}.{self.tablename}_custom;")
            customtable = self.cursor.fetchall()
            print(self.bcolors.OK+ f"Primeras filas de la custom table.." +self.bcolors.RESET)
            time.sleep(4)
            for fila in customtable:
                time.sleep(2)
                print(fila)

            print(self.bcolors.RED+ "Coinciden los PK actualizados e insertados...!" +self.bcolors.RESET)
            time.sleep(4)


            print('Delete table..')
            table.delete()
            print('Delete custom table..')
            custom_table.delete()
            print('Delete trigger..')
            trigger.delete()
            print('Delete db..')
            schema.delete()


        except Exception  as e:
            print(f"Error: {e}")

        self.conn.commit()

    

if __name__ == "__main__":
    obj = TriggerProcess()
    obj.run()
    print("Finish TriggerProcess")