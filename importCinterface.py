import pandas as pd
import psycopg2
import tkinter as tk
from tkinter import messagebox, filedialog
from itertools import cycle
from time import sleep
import params
def criar_tabela(conn, df, table_name):
    try:
        cursor = conn.cursor()
        cols = df.columns
        col_definitions = [f'"{col}" VARCHAR(10000)' for col in cols]
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(col_definitions)}
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Tabela '{table_name}' criada ou já existente.")
        cursor.close()
    
    except (Exception, psycopg2.DatabaseError) as error:
        messagebox.showerror("Erro de Banco de Dados", f"Erro ao criar a tabela: {error}")
        if conn:
            conn.rollback()

def importador(csv_file, table_name, db_params):
    try:
        df = pd.read_csv(csv_file)
        messagebox.showinfo("Status", "CSV lido com sucesso.")
        
        conn = psycopg2.connect(**db_params) 
        messagebox.showinfo("Status", "Conectado ao banco de dados.")
        
        cursor = conn.cursor()
        criar_tabela(conn, df, table_name)
        
        # Truncar tabela
        truncate_query = f"TRUNCATE TABLE {table_name};"
        cursor.execute(truncate_query)
        
        # Pega colunas do dataframe
        cols = df.columns.tolist()
        columns = ", ".join([f'"{c}"' for c in cols])
        placeholders = ", ".join(["%s"] * len(cols))
        insert_query = f" INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        for i, row in df.iterrows():
            try:
                cursor.execute(insert_query, tuple(row))
            except Exception as e:
                print(f"Linha problemática: {row.values}")
                conn.rollback()
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        messagebox.showinfo("Sucesso", "Importação concluída com sucesso!")
        
    except FileNotFoundError:
        messagebox.showerror("Erro", f"Arquivo '{csv_file}' não encontrado.")
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro: {e}")

import tkinter as tk
from tkinter import filedialog, messagebox
from itertools import cycle

# Configuração da interface 
def criar_interface():
    root = tk.Tk()
    root.title("Importador de CSV TechLab")
    root.geometry("600x400")

    # carrega frames do gif
    frames = [tk.PhotoImage(file="animacao.gif", format=f"gif -index {i}") for i in range(30)]
    frames_cycle = cycle(frames)

    # Função da animação
    def animar(widget, count=0, callback=None):
        try:
            frame = next(frames_cycle)
            widget.config(image=frame, text="")
            count += 1

            if count < 45:  # número de frames
                root.after(25, animar, widget, count, callback)
            else:
                widget.config(image="", text="Importar Dados")  # volta ao normal
                if callback:
                    callback()  # executa o que for passado depois da animação
        except:
            pass

    # Parâmetros do banco de dados
    db_params = params.db_params

    # Função de importação
    def on_import_click():
        csv_file = filedialog.askopenfilename()

        table_name = csv_file[csv_file.rfind("/")+1:-4:]
        if not csv_file.endswith(".csv"):
            messagebox.showwarning("Atenção", "Selecione um arquivo .csv.")
            return

        importador(csv_file, table_name, db_params)

    # Quando clica no botão → roda explosão → depois chama importador
    def on_button_click():
        animar(button_import, callback=on_import_click)

    # Botão de importação
    button_import = tk.Button(root, text="Importar Dados", command=on_button_click)
    button_import.pack(pady=20)

    root.mainloop()


# Inicia a aplicação
if __name__ == "__main__":
    criar_interface()



