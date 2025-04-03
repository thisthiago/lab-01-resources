import psycopg2
import random
from datetime import datetime, timedelta
from faker import Faker

NUM_CLIENTES = 500
fake = Faker('pt_BR')

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,  
    "database": "barbearia_db",
    "user": "admin",
    "password": "senhasegura"  
}

from faker import Faker

observacoes = ['Prefere cortes curtos', 'Alérgico a produtos com álcool', 
              'Gosta de conversar durante o corte', 'Cliente desde a inauguração',
              'Tem preferência por profissional específico', 'Sempre traz o filho',
              'Faz corte mensalmente', 'Tem medo de navalha', 'Prefere cortes tradicionais',
              'Não gosta de máquina', 'Prefere atendimento rápido']

fake = Faker('pt_BR')

def gerar_nome():
    """Gera um nome completo realista usando Faker"""
    if random.random() < 0.7:
        nome = fake.first_name_male()
    else:
        nome = fake.first_name_female()
    
    sobrenome = fake.last_name()
    
    if random.random() < 0.3:
        sobrenome += " " + fake.last_name()
    
    if random.random() < 0.1:
        prefixos = ['Dr.', 'Sr.', 'Prof.']
        if random.random() < 0.5: 
            nome = random.choice(prefixos) + " " + nome
        else:  
            sufixos = ['Jr.', 'Filho', 'Neto']
            sobrenome += " " + random.choice(sufixos)
    
    return nome,sobrenome

def conectar():
    """Estabelece conexão com o banco de dados"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Conexão estabelecida com sucesso!")
        return conn
    except psycopg2.Error as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None

def gerar_cliente(nome,sobrenome):   
    nome_completo = f"{nome} {sobrenome}"
    
    telefone = f"(11) 9{random.randint(10, 99)}-{random.randint(1000, 9999)}"
    
    email = f"{nome.lower()}.{sobrenome.lower()}{random.choice(['', str(random.randint(1, 99))])}@{random.choice(['gmail.com', 'hotmail.com', 'outlook.com'])}"
    
    idade = random.randint(18, 80)
    data_nascimento = datetime.now() - timedelta(days=idade*365 + random.randint(0, 364))
    
    dias_cadastro = random.randint(1, 5*365)
    data_cadastro = datetime.now() - timedelta(days=dias_cadastro)
    
    obs = random.choice(observacoes) if random.random() < 0.25 else None
    
    ativo = random.random() < 0.9
    
    return (nome_completo, telefone, email, data_nascimento.date(), data_cadastro, obs, ativo)

def inserir_clientes():
    conn = conectar()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        #cur.execute("TRUNCATE TABLE cliente RESTART IDENTITY;")
        
        for _ in range(NUM_CLIENTES):
            nome,sobrenome = gerar_nome()
            cliente = gerar_cliente(nome,sobrenome)
            cur.execute("""
                INSERT INTO cliente (nome, telefone, email, data_nascimento, data_cadastro, observacoes, ativo)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, cliente)
        
        conn.commit()
        print(f"{NUM_CLIENTES} clientes inseridos com sucesso!")
        
    except Exception as e:
        conn.rollback()
        print(f"Erro ao inserir dados: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    inserir_clientes()