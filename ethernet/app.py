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
    transfer_instances = []
    for ip, maq in lectura_nodos:
        transfer_instances.append(readTransfer(ip, maq))
    
    print("--- Iniciando ciclo de lectura principal ---")

    try:
        while True: # Este es el ÚNICO bucle infinito
            print(f"\n--- Ejecutando ciclo de lectura para {len(transfer_instances)} máquinas ---")
            
            for rdTransfer_instance in transfer_instances:
                # Cada instancia se conecta y lee SUS propios nodos
                print(f"--- Leyendo máquina: {rdTransfer_instance.codmaq} ---") 
                rdTransfer_instance.connect_opcua() 
            
            time.sleep(0.1) # Espera un poco antes de la siguiente ronda de lecturas de TODAS las máquinas

    except KeyboardInterrupt:
        print("\nCiclo de lectura detenido por el usuario.")
    except Exception as e:
        print(f"Ocurrió un error inesperado en el ciclo principal: {e}")

    # Opcional: Imprimir los conteos finales después de la interrupción
    print("\n--- Resumen final de cambios detectados ---")
    for rdTransfer_instance in transfer_instances:
        print(f"Máquina {rdTransfer_instance.codmaq}:")
        print(f"  Total de cambios detectados: {rdTransfer_instance.change_count}")
        # print(f"  Valores finales almacenados: {rdTransfer_instance.previous_values}") # Descomentar para depuración

main()