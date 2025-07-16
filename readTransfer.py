from opcua import Client
from database import Database
import time

class readTransfer:
    def __init__(self, direccion, codmaq):
        self.direccion = f'{direccion}:4840'
        self.db = Database()
        self.response_list = []
        self.codmaq = codmaq
        self.previous_values = {}
        self.change_count = 0

    def connect_database(self):
        try:
            if self.db:
                print('conexion existosa a la base de datos')
                query = f"""
                        SELECT tag, nodo
                        FROM opcua.ethernet_transfer
                        WHERE tag
                        LIKE '%{self.codmaq}%'
                        """

                response_database = self.db.get_query(query)
                self.response_list = [] 
                for data in response_database:
                    tag = data[0]
                    nodo = data[1]
                    opc_dic = {
                        'tag': tag,
                        'nodo': nodo
                    }
                    #print(f'tag:{tag} - nodo:{nodo}')
                    self.response_list.append(opc_dic)
                
                print(f"Se han cargado {len(self.response_list)} registros.")
                return True # Retorna True para indicar éxito
            else:
                print('Problemas de conexión a la base de datos.')
                return False # Retorna False para indicar fallo

        except Exception as e:
            print(f"Problemas con la funcion conexion databse {e}")

    def connect_opcua(self):
        if not self.response_list:
            print(f"La lista de tags y nodos para '{self.codmaq}' está vacía. Intentando cargar desde la base de datos...")
            if not self.connect_database():
                print(f"No se pudieron cargar los datos de la base de datos para OPC UA (codmaq: {self.codmaq}). Saliendo.")
                return

        client = None
        try:
            client = Client(self.direccion)
            client.connect()
            print(f"Conexión exitosa al servidor OPC UA en {self.direccion}.")

            nodo_disparador = "ns=4;i=42"
            valor_disparador_actual = None

            # Obtener el nodo disparador
            nodo_obj = client.get_node(nodo_disparador)
            valor_disparador_actual = nodo_obj.get_value()
            print(f"Valor del nodo disparador: {valor_disparador_actual}")

            # Solo actuar si valor es 1 o 2
            if valor_disparador_actual in [1, 2]:
                print(f"Valor válido detectado en nodo disparador: {valor_disparador_actual}")

                # Capturamos todos los valores actuales
                valores_capturados = {}
                for data_bd in self.response_list:
                    tag = data_bd['tag']
                    nodo = data_bd['nodo']
                    try:
                        valor = client.get_node(nodo).get_value()
                        valores_capturados[tag] = valor
                    except Exception as e:
                        print(f"Error al leer nodo {nodo}: {e}")

                # Si el valor es 1, esperar 5 segundos por si llega a 2
                if valor_disparador_actual == 1:
                    print("Esperando 5 segundos para ver si cambia a 2...")
                    tiempo_inicial = time.time()
                    llego_a_2 = False

                    while time.time() - tiempo_inicial < 5:
                        nuevo_valor = nodo_obj.get_value()
                        if nuevo_valor == 2:
                            print("Cambio a 2 detectado dentro de los 5 segundos.")
                            valor_disparador_actual = 2
                            llego_a_2 = True
                            break
                        time.sleep(0.5)  # Espera intermedia para no saturar

                    if not llego_a_2:
                        print("No llegó a 2 dentro del tiempo, se usará el valor 1 para insertar.")

                # Insertar todos los valores con el valor_disparador_actual (1 o 2)
                for tag, valor in valores_capturados.items():
                    self.db.proccess_data_transfer(tag, valor, 'ethernetmaq.tranfers_cycle_07_2025')
                self.change_count += 1
                print(f"Datos insertados con trigger = {valor_disparador_actual}")

            else:
                print("El valor del nodo disparador no es 1 ni 2. No se insertan datos.")

            client.disconnect()
            print(f"Desconectado del servidor OPC UA para {self.codmaq}. Total de inserciones: {self.change_count}")

        except Exception as e:
            print(f"Error en connect_opcua para {self.codmaq}: {e}")

    def loop(self):
        while True:
            self.connect_opcua()
            #time.sleep(0.1)
