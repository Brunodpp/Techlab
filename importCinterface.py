import pandas as pd
import psycopg2
import tkinter as tk
from tkinter import messagebox, filedialog
from itertools import cycle
from time import sleep
import os
#funcao switch para tipos
def sw(tipo,coluna,df):
    max_len=1000
    if tipo == 'object':
        tamanho = df[coluna].str.len()
        max_len = int(tamanho.max())
    switcher={
        'int64': 'INT',
        'float64': 'FLOAT',
        'object': f'VARCHAR({max_len})',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }
    return switcher.get(tipo, 'VARCHAR(1000)')
def criar_tabela(conn, df, table_name):
    try:
        cursor = conn.cursor()
        tipos = df.dtypes
        cols = df.columns
        col_definitions = [f'"{cols[col]}" {sw(str(tipos.iloc[col]),cols[col],df)}' for col in range(len(cols))]
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
    problematic_rows = []
    try:
        df = pd.read_csv(
            csv_file,
            sep=None,
            engine="python",
            
            encoding="UTF-8",
    
            )

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
                problematic_rows.append(row)
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
    finally:
       
        if problematic_rows:
            
            df_problematic = pd.DataFrame(problematic_rows, columns=df.columns)
            output_csv_file = csv_file.replace(".csv", "_linhas_com_erro.csv")
            df_problematic.to_csv(output_csv_file, index=False)

            messagebox.showwarning(
                "Linhas com Erro", 
                f"Foram encontradas e salvas {len(problematic_rows)} linhas com erro no arquivo:\n{output_csv_file}"
            )
        else:
            messagebox.showinfo("Tudo Certo", "Nenhuma linha problemática encontrada. Importação perfeita!")

import tkinter as tk
from tkinter import filedialog, messagebox,ttk
from itertools import cycle

# Configuração da interface 
def criar_interface():
    root = tk.Tk()
    root.title("Importador de CSV TechLab")
    root.geometry("600x400")
    
    

   

    # estruturando de db params com placeholders
    db_params = {
        'host': 'placeholder',
        'database': 'placeholder',
        'user': 'placeholder',
        'password': 'placeholder'
    }
    def ler_pasta():
        try:
            pasta=filedialog.askdirectory()
            caminhos=[pasta+"/"+nome for nome in os.listdir(pasta)]
            arquivos = [arq for arq in caminhos if os.path.isfile(arq)]
            csv = [arq for arq in arquivos if arq.lower().endswith(".csv")]
            for i in csv:
                table_name = i[i.rfind("/")+1:-4:]
                importador(i, table_name, db_params)
        except FileNotFoundError:
            messagebox.showerror("Erro", f"Pasta '{pasta}' não encontrada.")


    # Função de importação
    def on_import_click():
        csv_file = filedialog.askopenfilename()

        table_name = csv_file[csv_file.rfind("/")+1:-4:]
        if not csv_file.endswith(".csv"):
            messagebox.showwarning("Atenção", "Selecione um arquivo .csv.")
            return

        importador(csv_file, table_name, db_params)

   

    #funcao para setar o db
    def setdb():
        db_params['host']=entry_host.get()
        db_params['database']=entry_db.get()
        db_params['user']=entry_user.get()
        db_params['password']=entry_pw.get()
        messagebox.showinfo("Status", "Configurações salvas com sucesso.")
    teste=ttk.Notebook(root)
    teste.place(x=0,y=0,width=600,height=400)
    #aba 1 Importador
    aba1=tk.Frame(teste)
    teste.add(aba1,text="Importar")
    button_import = tk.Button(aba1, text="Importar Arquivo", command=on_import_click)
    button_import.pack(pady=20)
    button_importp = tk.Button(aba1, text="Importar Pasta", command=ler_pasta)
    button_importp.pack(pady=20)
    #aba 2 db config
    aba2=tk.Frame(teste)
    teste.add(aba2,text="Config")
    label_host = tk.Label(aba2, text="Nome do host:")
    label_host.pack(pady=5)
    entry_host = tk.Entry(aba2, width=50)
    entry_host.pack(pady=5)
    
   
    label_db = tk.Label(aba2, text="Nome da Base de Dados:")
    label_db.pack(pady=5)
    entry_db = tk.Entry(aba2, width=50)
    entry_db.pack(pady=5)

    label_user = tk.Label(aba2, text="Nome de Usuário:")
    label_user.pack(pady=5)
    entry_user = tk.Entry(aba2, width=50)
    entry_user.pack(pady=5)

    label_pw = tk.Label(aba2, text="Senha:")
    label_pw.pack(pady=5)
    entry_pw = tk.Entry(aba2, width=50)
    entry_pw.pack(pady=5)

    button_db = tk.Button(aba2, text="Salvar", command=setdb)
    button_db.pack(pady=20)
    
    
    
    
    
    root.mainloop()


# Inicia a aplicação
if __name__ == "__main__":
    criar_interface()



