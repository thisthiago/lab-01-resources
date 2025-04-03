# Laboratório PySpark para Análise de Barbearia

## Visão Geral

Este repositório contém dois casos de estudo completos para análise de dados de uma barbearia usando PySpark:

1. **PySpark DataFrame API**
2. **Spark SQL**

Ambos os casos utilizam o mesmo banco de dados PostgreSQL como fonte de dados, demonstrando abordagens diferentes para resolver problemas similares.

## Pré-requisitos

- Python 3.8+
- Apache Spark 3.0+
- PostgreSQL 12+
- Driver JDBC para PostgreSQL
- Biblioteca PySpark
- Biblioteca python-dotenv (para gerenciamento de credenciais)

## Configuração do Ambiente

1. Clone este repositório:
   ```
    docker network create lab-01-network

    docker network ls

    docker run -d --network lab-01-network --name minio -p 9000:9000 -p 9001:9001 -v C:/minio/data:/data -e "MINIO_ROOT_USER=admin" -e "MINIO_ROOT_PASSWORD=senhasegura" quay.io/minio/minio server /data --console-address ":9001"

    docker run -d --network lab-01-network --name zeppelin  -p 8080:8080 thisthiago/zeppelin-spark-delta:latest

    docker run -d --network lab-01-network --name jupyter  -p 8888:8888 thisthiago/jupyter:latest

    docker run -d --name postgres --network lab-01-network -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=senhasegura -v C:/postgres-data:/var/lib/postgresql/data -p 5433:5432 postgres:13
    ```

## Caso 1: PySpark DataFrame API

### Arquivo Principal
`dataframe_api_case.py`

### Recursos Demonstrados:
- Leitura de dados do PostgreSQL
- Transformações com DataFrame API
- Operações de agregação
- Joins entre tabelas
- Escrita de resultados em múltiplos formatos

### Como Executar:
```bash
spark-submit dataframe_api_case.py
```

### Principais Funcionalidades:
```python
# Exemplo de operação
df_clientes.filter(col("ativo") == True) \
           .groupBy("faixa_etaria") \
           .agg(count("*").alias("total_clientes")) \
           .show()
```

## Caso 2: Spark SQL

### Arquivo Principal
`spark_sql_case.py`

### Recursos Demonstrados:
- Criação de views temporárias
- Consultas SQL completas
- CTEs (Common Table Expressions)
- Funções SQL avançadas
- Otimização de consultas

### Como Executar:
```bash
spark-submit spark_sql_case.py
```

### Principais Funcionalidades:
```sql
-- Exemplo de consulta
WITH metrics AS (
  SELECT profissional_id, COUNT(*) as total_agendamentos
  FROM agendamentos
  WHERE status = 'concluido'
  GROUP BY profissional_id
)
SELECT * FROM metrics ORDER BY total_agendamentos DESC;
```

## Comparação das Abordagens

| Característica          | DataFrame API                     | Spark SQL                        |
|-------------------------|-----------------------------------|----------------------------------|
| Sintaxe                 | Programática (métodos encadeados) | Declarativa (SQL tradicional)    |
| Flexibilidade           | Alta (todas operações do Spark)   | Limitada à sintaxe SQL           |
| Performance             | Igual (mesmo engine de execução)  | Igual                            |
| Legibilidade            | Depende do desenvolvedor          | Familiar para analistas SQL      |
| Uso de UDFs             | Mais simples                      | Requer registro adicional        |

## Estrutura do Banco de Dados

O banco de dados PostgreSQL contém as seguintes tabelas:
- `cliente` - Informações dos clientes
- `profissional` - Dados dos barbeiros/cabeleireiros
- `servico` - Tipos de serviços oferecidos
- `agendamento` - Registros de agendamentos
- `pagamento` - Informações de pagamento
- `horario_profissional` - Horários de trabalho dos profissionais

## Resultados Esperados

Ambos os casos gerarão:
1. Análise de agendamentos por período
2. Relatório de performance dos profissionais
3. Estatísticas de serviços mais populares
4. Exportação dos resultados em formato CSV e Parquet