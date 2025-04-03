import psycopg2
import random
from datetime import datetime, timedelta
from faker import Faker
from collections import defaultdict
from decimal import Decimal

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "barbearia_db",
    "user": "admin",
    "password": "senhasegura" 
}

fake = Faker('pt_BR')

def conectar():
    """Estabelece conexão com o banco de dados"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None

def validar_data(data_str):
    """Valida e converte string de data no formato dd/mm/aaaa"""
    try:
        return datetime.strptime(data_str, "%d/%m/%Y")
    except ValueError:
        print("Formato de data inválido. Use dd/mm/aaaa")
        return None

def gerar_agendamentos_com_minimo(data_inicio, data_fim):
    """Gera agendamentos garantindo mínimo de 25 por dia útil"""
    conn = conectar()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            # Obtém dados necessários
            cur.execute("SELECT cliente_id FROM cliente WHERE ativo = TRUE")
            clientes = [row[0] for row in cur.fetchall()]
            
            cur.execute("""
                SELECT p.profissional_id, p.nome, 
                       hp.dia_semana, hp.hora_inicio, hp.hora_fim 
                FROM profissional p
                JOIN horario_profissional hp ON p.profissional_id = hp.profissional_id
                WHERE p.ativo = TRUE AND hp.ativo = TRUE
                ORDER BY p.profissional_id, hp.dia_semana
            """)
            
            # Organiza horários por profissional
            profissionais = defaultdict(list)
            for row in cur.fetchall():
                prof_id, nome, dia_semana, hora_inicio, hora_fim = row
                profissionais[prof_id].append({
                    'dia_semana': dia_semana,
                    'hora_inicio': hora_inicio,
                    'hora_fim': hora_fim,
                    'nome': nome
                })

            cur.execute("SELECT servico_id, duracao_estimada, preco FROM servico WHERE ativo = TRUE")
            servicos = [{"id": row[0], "duracao": row[1], "preco": row[2]} for row in cur.fetchall()]

            if not clientes or not profissionais or not servicos:
                print("Dados insuficientes para gerar agendamentos.")
                return

            # Valida datas
            inicio = validar_data(data_inicio)
            fim = validar_data(data_fim)
            if not inicio or not fim or fim <= inicio:
                print("Datas inválidas. A data final deve ser após a inicial.")
                return

            print(f"\nGerando agendamentos entre {inicio.strftime('%d/%m/%Y')} e {fim.strftime('%d/%m/%Y')}...")

            # Dicionário para controlar slots ocupados
            slots_ocupados = defaultdict(set)
            total_agendamentos = 0

            # Para cada dia no período
            delta_dias = (fim - inicio).days + 1
            for dia in range(delta_dias):
                data_atual = inicio + timedelta(days=dia)
                dia_semana = data_atual.weekday()  # 0=segunda, 6=domingo

                # Pula domingos
                if dia_semana == 6:
                    print(f"Domingo {data_atual.strftime('%d/%m/%Y')} - pulando")
                    continue

                # Profissionais que trabalham neste dia
                profs_disponiveis = [
                    (prof_id, info) 
                    for prof_id, horarios in profissionais.items()
                    for info in horarios 
                    if info['dia_semana'] == dia_semana + 1  # Ajuste para 1=domingo no banco
                ]

                if not profs_disponiveis:
                    print(f"Dia {data_atual.strftime('%d/%m/%Y')} - nenhum profissional disponível")
                    continue

                # Meta fixa de 25 agendamentos por dia + aleatoriedade
                meta_agendamentos = max(75, random.randint(70, 122))  # Mínimo 25, máximo 35
                agendamentos_dia = 0

                # Tentar até atingir a meta ou esgotar slots
                tentativas_max = meta_agendamentos * 3  # Limite de tentativas
                tentativas = 0

                while agendamentos_dia < meta_agendamentos and tentativas < tentativas_max:
                    tentativas += 1
                    
                    # Escolhe profissional aleatório que trabalha nesse dia
                    prof_id, info_prof = random.choice(profs_disponiveis)
                    
                    # Gera horário dentro do turno do profissional
                    hora_inicio = info_prof['hora_inicio']
                    hora_fim = info_prof['hora_fim']
                    
                    hora = random.randint(hora_inicio.hour, hora_fim.hour - 1)
                    minuto = random.choice([0, 15, 30, 45])
                    data_hora = datetime(
                        year=data_atual.year,
                        month=data_atual.month,
                        day=data_atual.day,
                        hour=hora,
                        minute=minuto
                    )

                    # Verifica se o slot está livre
                    slot_key = (prof_id, data_hora)
                    if slot_key in slots_ocupados[prof_id]:
                        continue

                    # Escolhe serviço e cliente
                    servico = random.choice(servicos)
                    cliente_id = random.choice(clientes)

                    # Status (80% confirmado, 15% concluído, 5% cancelado)
                    status = random.choices(
                        ['confirmado', 'concluido', 'cancelado'],
                        weights=[80, 15, 5]
                    )[0]

                    # Insere agendamento
                    cur.execute("""
                        INSERT INTO agendamento (
                            cliente_id, profissional_id, servico_id, 
                            data_hora, duracao, status, 
                            data_criacao, data_atualizacao
                        ) 
                        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                        RETURNING agendamento_id
                    """, (
                        cliente_id, prof_id, servico['id'],
                        data_hora, servico['duracao'], status
                    ))

                    agendamento_id = cur.fetchone()[0]
                    slots_ocupados[prof_id].add(slot_key)
                    agendamentos_dia += 1
                    total_agendamentos += 1

                    # Gera pagamento se aplicável
                    if status in ['confirmado', 'concluido']:
                        formas_pagamento = ['dinheiro', 'cartao_credito', 'cartao_debito', 'pix']
                        forma_pagamento = random.choice(formas_pagamento)
                        
                        # 20% de chance de desconto de 10%
                        valor_total = float(servico['preco']) * (0.9 if random.random() < 0.2 else 1)
                        
                        # Status do pagamento
                        status_pagamento = 'pago' if (
                            status == 'concluido' or 
                            (status == 'confirmado' and random.random() < 0.7)
                        ) else 'pendente'
                        
                        cur.execute("""
                            INSERT INTO pagamento (
                                agendamento_id, valor_total, forma_pagamento, 
                                status, data_pagamento
                            ) 
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            agendamento_id, round(valor_total, 2),
                            forma_pagamento, status_pagamento,
                            data_hora if status_pagamento == 'pago' else None
                        ))

                print(f"Dia {data_atual.strftime('%d/%m/%Y')}: {agendamentos_dia} agendamentos (meta: {meta_agendamentos})")

            conn.commit()
            print(f"\n✅ Total de {total_agendamentos} agendamentos gerados com sucesso!")
            dias_uteis = delta_dias - (delta_dias // 7)  # Desconta os domingos
            print(f"   - Média de {total_agendamentos/dias_uteis:.1f} agendamentos/dia útil")
            
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro durante a geração: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    print("=== Gerador de Agendamentos com Mínimo Diário ===")
    data_inicio = input("Data inicial (dd/mm/aaaa): ").strip()
    data_fim = input("Data final (dd/mm/aaaa): ").strip()
    
    gerar_agendamentos_com_minimo(data_inicio, data_fim)