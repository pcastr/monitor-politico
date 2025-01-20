import json
import os

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from lancedb.pydantic import pydantic_to_schema

from app.ingestion.IGTAO_API_DEPUTADOS.models.model_deputados import Dados

# Caminho do arquivo JSON e do diretório onde salvar a tabela Delta
json_file_path = './data/deputados/PB/deputados_pb.json'
delta_table_path = './data/deputados/PB/delta/deputadostable'

# Definindo o esquema dos dados
schema = pa.schema([
    ('id', pa.int64()),
    ('uri', pa.string()),
    ('nome', pa.string()),
    ('siglaPartido', pa.string()),
    ('uriPartido', pa.string()),
    ('siglaUf', pa.string()),
    ('idLegislatura', pa.int32()),
    ('urlFoto', pa.string()),
    ('email', pa.string()),
])

schema = pydantic_to_schema(Dados)

print(schema)


# Função para adicionar novos dados na tabela Delta
def update_delta_table():
    # Lê o arquivo JSON
    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

    # Verifica se já existe a tabela Delta
    if os.path.exists(delta_table_path):
        print('Tabela Delta existente. Adicionando novos dados...')
    else:
        print('Tabela Delta não encontrada. Criando nova tabela...')

    # Transformando os dados em um formato que o Delta Lake entende
    dados = json_data['dados']
    arrow_data = []
    for item in dados:
        arrow_data.append([
            item.get('id', None),
            item.get('uri', None),
            item.get('nome', None),
            item.get('siglaPartido', None),
            item.get('uriPartido', None),
            item.get('siglaUf', None),
            item.get('idLegislatura', None),
            item.get('urlFoto', None),
            item.get('email', None),
        ])

    # Criando um Table do PyArrow
    table = pa.Table.from_arrays(
        [pa.array(col) for col in zip(*arrow_data)], schema=schema
    )

    # Verificando dados existentes na tabela Delta
    if os.path.exists(delta_table_path):
        delta_table = DeltaTable(delta_table_path)
        existing_df = delta_table.to_pandas()
        existing_ids = existing_df['id'].tolist()

        # Filtra dados novos que ainda não estão na tabela Delta
        new_data = [item for item in arrow_data if item[0] not in existing_ids]
        if new_data:
            new_table = pa.Table.from_arrays(
                [pa.array(col) for col in zip(*new_data)], schema=schema
            )
            write_deltalake(delta_table_path, new_table, mode='append')
            print('Novos dados adicionados com sucesso!')
        else:
            print('Nenhum dado novo para adicionar.')

    else:
        # Se a tabela Delta não existe, cria uma nova
        write_deltalake(delta_table_path, table, mode='overwrite')
        print('Tabela Delta criada e dados adicionados com sucesso!')


# Chamando a função para rodar o processo
update_delta_table()

# Lendo os dados para verificar
delta_table = DeltaTable(delta_table_path)
df = delta_table.to_pandas()

# Configurar pandas para exibir todas as linhas e colunas
pd.set_option('display.max_rows', None)  # Exibir todas as linhas
pd.set_option('display.max_columns', None)  # Exibir todas as colunas
pd.set_option('display.width', None)  # Ajuste para largura
pd.set_option(
    'display.max_colwidth', None
)  # Exibir todo o conteúdo das colunas

# Exibir o DataFrame
print(df)
