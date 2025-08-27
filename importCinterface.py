import pandas as pd
import psycopg2
import tkinter as tk
from tkinter import messagebox

def criar_tabela(conn, df, table_name):
    try:
        cursor = conn.cursor()
        cols = df.columns
        col_definitions = [f'"{col}" VARCHAR(255)' for col in cols]
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

# Configuração da interface 
def criar_interface():
    root = tk.Tk()
    root.title("Importador de CSV TechLab")
    root.geometry("600x400")
   

    # Parâmetros do banco de dados
    db_params = {
        'host': 'localhost',
        'database': 'postgres',
        'user': 'postgres_admin',
        'password': '12345'
    }

    # Funcao que sera chamada ao clicar no botao
    def on_import_click():
        csv_file = entry_csv.get()
        table_name = entry_table.get()
        if not csv_file or not table_name:
            messagebox.showwarning("Atenção", "Por favor, preencha todos os campos.")
            return

        importador(csv_file, table_name, db_params)

    
    label_csv = tk.Label(root, text="Nome do Arquivo CSV:")
    label_csv.pack(pady=5)
    entry_csv = tk.Entry(root, width=50)
    entry_csv.pack(pady=5)
    
   
    label_table = tk.Label(root, text="Nome da Tabela:")
    label_table.pack(pady=5)
    entry_table = tk.Entry(root, width=50)
    entry_table.pack(pady=5)

    # Botão de importação
    button_import = tk.Button(root, text="Importar Dados", command=on_import_click)
    button_import.pack(pady=20)
    
    root.mainloop()




# Inicia a aplicação
if __name__ == "__main__":
    criar_interface()



