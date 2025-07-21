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
                        FROM system_communication.ethernet
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
        # If response_list is empty, try to load data from the database
        if not self.response_list:
            print(f"La lista de tags y nodos para '{self.codmaq}' está vacía. Intentando cargar desde la base de datos...")
            if not self.connect_database():
                print(f"No se pudieron cargar los datos de la base de datos para OPC UA (codmaq: {self.codmaq}). Saliendo.")
                return

        client = None
        try:
            client = Client(self.direccion)
            client.connect()
            print(f"Conexión exitosa al servidor OPC UA en {self.direccion} -- {self.codmaq}.")
            
            # Note: self.change_count is cumulative across loop calls.
            # Uncomment the line below if you want to reset it for each connect_opcua call.
            # self.change_count = 0 

            for data_bd in self.response_list:
                tag_name = data_bd['tag']
                node_id = data_bd['nodo']

                try:
                    node = client.get_node(node_id)
                    current_value = node.get_value()
                    
                    # Check if this tag has a previous value stored
                    if tag_name in self.previous_values:
                        previous_value = self.previous_values[tag_name] # Correctly retrieve previous value
                        if current_value != previous_value:
                            self.change_count += 1 # Re-enabled the counter increment
                            print(f'CAMBIO DETECTADO: Tag: {tag_name} - Anterior: {previous_value}, Actual: {current_value}')
                            self.db.proccess_data_transfer(tag_name, current_value, self.codmaq)
                        else:
                            print(f'Tag: {tag_name} - Valor: {current_value} (sin cambios)')
                    else:
                        # First time reading this tag, no previous value to compare
                        print(f'Tag: {tag_name} - Valor inicial (primera lectura): {current_value}')

                    # Update the previous value for the next comparison
                    self.previous_values[tag_name] = current_value

                except Exception as node_e:
                    print(f"Error al leer el nodo {node_id} ({tag_name}): {node_e}")

            print(f"\nConteo total de cambios detectados para {self.codmaq}: {self.change_count}")
            print(f"Estado actual de previous_values para {self.codmaq}: {self.previous_values}")
            client.disconnect()
        except Exception as e:
            print(f'Error al conectar OPC UA para {self.codmaq}: {e}')
        #finally:
        #    if client:
        #        client.disconnect()
        #        print(f"Desconectado del servidor OPC UA para {self.codmaq}.")
    
    def loop(self):
        while True:
            self.connect_opcua()
            time.sleep(5)
