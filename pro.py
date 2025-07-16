from ReadOpcua import readTransfer
import time

def lectura_texto():
    address_array = []
    with open('puerto_maquina.txt', 'r') as address:
        for linea in address:
            linea = linea.strip()
            if linea:
                parte = linea.split(',',1)
                ip = parte[0].strip()
                maq = parte[1].strip()
                address_array.append((ip, maq))
        print(address_array)
    return address_array

def main():
    lectura_nodos = lectura_texto()
    for ip, maq in lectura_nodos:
        rdTransfer = readTransfer(ip, maq)
        print("--- Iniciando ciclo de lectura principal ---")

        try:
            while True:
                print("\n--- Ejecutando ciclo de TRANFERS ---")
                rdTransfer.connect_opcua()
                
                #print("\n--- Ejecutando ciclo para TTV 002E ---")
                #tranfer2_instance.connect_opcua()

                #print("\n--- Ejecutando ciclo para TTV 004E ---")
                #tranfer4_instance.connect_opcua()
                
                time.sleep(0.1) # Espera 5 segundos antes de la siguiente ronda de lecturas

        except KeyboardInterrupt:
            print("\nCiclo de lectura detenido por el usuario.")

        print(f"\nFinal Total de cambios detectados para TRANSFERS: {rdTransfer.change_count}")
        print(f"Valores finales almacenados en previous_values para TRANSFERS: {rdTransfer.previous_values}")

main()