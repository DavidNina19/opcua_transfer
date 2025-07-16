import pymysql as sql
from datetime import datetime


class Database:

    def __init__(self, host=None, user=None, password=None, database=None, port=None):
        self.host = host or '192.168.252.35'
        self.user = user or 'powerbi_david'
        self.password = password or 'Scada2024%'
        self.database = database or 'system_communication'
        self.port = port or 3306

        self.connection = sql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port
        )
        self.cursor = self.connection.cursor()

    def viewTable(self, tablename):
        self.cursor.execute(f"SELECT * FROM {tablename}")
        self.connection.commit()
        return self.cursor.fetchall()

    def get_query(self, query):
        self.cursor.execute(query)
        self.connection.commit()
        return self.cursor.fetchall()

    def set_query(self, query, params=None):
        """Ejecuta una consulta INSERT, UPDATE o DELETE."""
        if not self.connection:
            raise Exception("No hay conexión")
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                self.connection.commit()
        except sql.MySQLError as e:
            print(f"Error al ejecutar la consulta: {e}")

    def close(self):
        self.cursor.close()
        self.connection.close()

    def table_exists(self, tabla):
        """Verifica si la tabla existe en la base de datos."""
        try:
            self.cursor.execute(f"SHOW TABLES LIKE '{tabla}'")
            return self.cursor.fetchone() is not None
        except Exception as e:
            print(f"Error al verificar si la tabla existe: {e}")
            return False

    def create_table(self, tabla):
        """Crea la tabla si no existe."""
        try:
            query = f"""
                CREATE TABLE {tabla} (
                    id INT NOT NULL AUTO_INCREMENT,
                    tipo_temp VARCHAR(100),
                    valor FLOAT,
                    t_stamp TIMESTAMP,
                    PRIMARY KEY (id)
                );
            """
            self.set_query(query)
            print(f"Tabla {tabla} creada exitosamente.")
        except Exception as e:
            print(f"Error al crear la tabla {tabla}: {e}")
    
    def create_data_general_table(self, table_name):

        try:
            create_query = f"""
            CREATE TABLE data_scada.{table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                codmaq VARCHAR(50) NOT NULL,
                nomMaquina VARCHAR(255) NOT NULL,
                idTipOrd VARCHAR(20),
                abrTipOrd2 VARCHAR(10),
                idNumOrd VARCHAR(20),
                cantidadOrden VARCHAR(20),
                desOperacion VARCHAR(255),
                itemProceso VARCHAR(20),
                inicio DATETIME,
                termino DATETIME,
                cantidadStd VARCHAR(4),
                operario VARCHAR(255),
                inicioParada DATETIME,
                cantidadReal VARCHAR(6),
                t_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            return self.set_query(create_query) # set_query ya maneja el commit y errores
        except Exception as e:
            print(f"Error al crear la tabla {table_name}: {e}")
            return False

    def proccess_data(self, tabla, tipo, valor):
        """Insertar datos en la tabla."""
        if not self.connection:
            print("No hay conexión a la base de datos.")
            return

        try:
            cursor = self.connection.cursor()
            # Obtener la marca de tiempo actual
            t_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            query = f"""
                    INSERT INTO {tabla} (tipo_temp, valor, t_stamp)
                    VALUES (%s, %s, %s)
                    """
            cursor.execute(query, (tipo, valor, t_stamp))
            self.connection.commit()
            print(
                f"Dato insertado: tipo={tipo}, valor={valor}, t_stamp={t_stamp}")
        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            if cursor:
                cursor.close()

    def proccess_data_realtime(self, area, maq, valor):
        """Insertar datos en la tabla."""
        if not self.connection:
            print("No hay conexión a la base de datos.")
            return

        try:
            cursor = self.connection.cursor()
            # t_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Obtener la marca de tiempo actual
            query = f"""
                    INSERT INTO data_scada.{area}_realtime (codmaq, valor, t_stamp)
                    VALUES (%s, %s, NOW())
                    """
            cursor.execute(query, (maq, valor))
            self.connection.commit()
            print(f"Dato insertado: maq={maq}, valor={valor}")
        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            if cursor:
                cursor.close()

    def proccess_data_transfer(self, codmaq, valor, tabla):
        """Insertar datos en la tabla."""
        if not self.connection:
            print("No hay conexión a la base de datos.")
            return

        try:
            cursor = self.connection.cursor()
            # t_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Obtener la marca de tiempo actual
            query = f"""
                    INSERT INTO {tabla} (codmaq, valor, t_stamp)
                    VALUES (%s, %s, NOW())
                    """
            cursor.execute(query, (codmaq, valor))
            self.connection.commit()
            print(f"Dato insertado: codmaq={codmaq}, valor={valor}")
        except Exception as e:
            print(f"Error al insertar datos: {e}")
        finally:
            if cursor:
                cursor.close()
