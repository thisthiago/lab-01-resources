-- Criação do banco de dados
-- CREATE DATABASE barbearia_db;

-- Tabela de Clientes
CREATE TABLE cliente (
    cliente_id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    telefone VARCHAR(20),
    email VARCHAR(100),
    data_nascimento DATE,
    data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    observacoes TEXT,
    ativo BOOLEAN DEFAULT TRUE
);

-- Tabela de Profissionais (Barbeiros/Cabeleireiros)
CREATE TABLE profissional (
    profissional_id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    telefone VARCHAR(20),
    email VARCHAR(100),
    especialidade VARCHAR(100),
    data_admissao DATE,
    ativo BOOLEAN DEFAULT TRUE
);

-- Tabela de Serviços/Tipos de Corte
CREATE TABLE servico (
    servico_id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    descricao TEXT,
    duracao_estimada INTEGER NOT NULL, -- em minutos
    preco NUMERIC(10, 2) NOT NULL,
    ativo BOOLEAN DEFAULT TRUE
);

-- Tabela de Agendamentos
CREATE TABLE agendamento (
    agendamento_id SERIAL PRIMARY KEY,
    cliente_id INTEGER NOT NULL REFERENCES cliente(cliente_id),
    profissional_id INTEGER NOT NULL REFERENCES profissional(profissional_id),
    servico_id INTEGER NOT NULL REFERENCES servico(servico_id),
    data_hora TIMESTAMP NOT NULL,
    duracao INTEGER NOT NULL, -- em minutos
    status VARCHAR(20) DEFAULT 'agendado' CHECK (status IN ('agendado', 'confirmado', 'cancelado', 'concluido', 'nao_compareceu')),
    observacoes TEXT,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Pagamentos
CREATE TABLE pagamento (
    pagamento_id SERIAL PRIMARY KEY,
    agendamento_id INTEGER REFERENCES agendamento(agendamento_id),
    valor_total NUMERIC(10, 2) NOT NULL,
    forma_pagamento VARCHAR(50) NOT NULL CHECK (forma_pagamento IN ('dinheiro', 'cartao_credito', 'cartao_debito', 'pix', 'transferencia')),
    status VARCHAR(20) DEFAULT 'pendente' CHECK (status IN ('pendente', 'pago', 'cancelado', 'reembolsado')),
    data_pagamento TIMESTAMP,
    observacoes TEXT
);

-- Tabela de Horários de Trabalho dos Profissionais
CREATE TABLE horario_profissional (
    horario_id SERIAL PRIMARY KEY,
    profissional_id INTEGER NOT NULL REFERENCES profissional(profissional_id),
    dia_semana INTEGER NOT NULL CHECK (dia_semana BETWEEN 1 AND 7), -- 1=Dom, 2=Seg, ..., 7=Sab
    hora_inicio TIME NOT NULL,
    hora_fim TIME NOT NULL,
    ativo BOOLEAN DEFAULT TRUE
);

-- Tabela de Promoções/Descontos
CREATE TABLE promocao (
    promocao_id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    descricao TEXT,
    desconto_percentual NUMERIC(5, 2),
    data_inicio DATE NOT NULL,
    data_fim DATE NOT NULL,
    ativo BOOLEAN DEFAULT TRUE
);

-- Tabela de Serviços com Promoção
CREATE TABLE servico_promocao (
    servico_promocao_id SERIAL PRIMARY KEY,
    servico_id INTEGER NOT NULL REFERENCES servico(servico_id),
    promocao_id INTEGER NOT NULL REFERENCES promocao(promocao_id),
    UNIQUE(servico_id, promocao_id)
);

-- Índices para melhorar performance
CREATE INDEX idx_agendamento_cliente ON agendamento(cliente_id);
CREATE INDEX idx_agendamento_profissional ON agendamento(profissional_id);
CREATE INDEX idx_agendamento_data ON agendamento(data_hora);
CREATE INDEX idx_pagamento_agendamento ON pagamento(agendamento_id);
CREATE INDEX idx_horario_profissional ON horario_profissional(profissional_id, dia_semana);