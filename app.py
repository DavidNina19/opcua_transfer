from readTransfer import readTransfer
import time

# Crea las instancias de readTransfer FUERA del bucle
# para que sus estados (self.previous_values) persistan.
tranfer1_instance = readTransfer('opc.tcp://192.168.146.151', '001E')
#tranfer2_instance = readTransfer('opc.tcp://192.168.146.153', '002E')
#tranfer4_instance = readTransfer('opc.tcp://192.168.252.35', '004E')

print("--- Iniciando ciclo de lectura principal ---")

try:
    while True:
        print("\n--- Ejecutando ciclo para TTV 001E ---")
        tranfer1_instance.connect_opcua()
        
        #print("\n--- Ejecutando ciclo para TTV 002E ---")
        #tranfer2_instance.connect_opcua()

        #print("\n--- Ejecutando ciclo para TTV 004E ---")
        #tranfer4_instance.connect_opcua()
        
        time.sleep(0.1) # Espera 5 segundos antes de la siguiente ronda de lecturas

except KeyboardInterrupt:
    print("\nCiclo de lectura detenido por el usuario.")

print(f"\nFinal Total de cambios detectados para TTV 001E: {tranfer1_instance.change_count}")
print(f"Valores finales almacenados en previous_values para TTV 001E: {tranfer1_instance.previous_values}")
#print(f"\nFinal Total de cambios detectados para TTV 002E: {tranfer2_instance.change_count}")
#print(f"Valores finales almacenados en previous_values para TTV 002E: {tranfer2_instance.previous_values}")
#print(f"\nFinal Total de cambios detectados para TTV 004E: {tranfer4_instance.change_count}")
#print(f"Valores finales almacenados en previous_values para TTV 004E: {tranfer4_instance.previous_values}")