import json
import os

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


def load_json(json_file_path):
    """
    Carrega os dados de um arquivo JSON.
    """
    with open(json_file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def transform_data(json_data, schema_fields):
    """
    Transforma os dados JSON em um formato compatível com PyArrow Table.
    """
    arrow_data = []
    for item in json_data['dados']:
        row = [item.get(field.name, None) for field in schema_fields]
        arrow_data.append(row)
    return arrow_data


def create_arrow_table(data, schema):
    """
    Cria uma tabela PyArrow a partir dos dados e do esquema.
    """
    columns = [pa.array(col) for col in zip(*data)]
    return pa.Table.from_arrays(columns, schema=schema)


def update_delta_table(json_file_path, delta_table_path, schema):
    """
    Atualiza ou cria uma tabela Delta a partir de um JSON, utilizando o
    esquema fornecido.
    """
    # Carrega os dados do JSON
    json_data = load_json(json_file_path)

    # Transforma os dados do JSON para PyArrow
    schema_fields = schema
    arrow_data = transform_data(json_data, schema_fields)
    table = create_arrow_table(arrow_data, schema)

    # Verifica se a tabela Delta já existe
    if os.path.exists(delta_table_path):
        print('Tabela Delta existente. Verificando novos dados...')
        delta_table = DeltaTable(delta_table_path)

        # Obtém dados existentes
        existing_df = delta_table.to_pandas()
        existing_ids = existing_df['id'].tolist()

        # Filtra novos dados
        new_data = [item for item in arrow_data if item[0] not in existing_ids]
        if new_data:
            new_table = create_arrow_table(new_data, schema)
            write_deltalake(delta_table_path, new_table, mode='append')
            print('Novos dados adicionados com sucesso!')
        else:
            print('Nenhum dado novo para adicionar.')
    else:
        print('Tabela Delta não encontrada. Criando nova tabela...')
        write_deltalake(delta_table_path, table, mode='append')
        print('Tabela Delta criada e dados adicionados com sucesso!')


# Exemplo de uso
if __name__ == '__main__':
    import pandas as pd

    from app.ingestion.common.converter import (
        PydanticToPyArrowConverter,
    )
    from app.ingestion.IGTAO_API_DEPUTADOS.models.model_deputados import Dados

    # Caminhos e esquema
    json_file_path = './data/deputados/PB/deputados_pb.json'
    delta_table_path = './data/deputados/PB/delta/deputadostable'
    schema = PydanticToPyArrowConverter.pydantic_to_schema(Dados)

    # Atualiza a tabela Delta
    update_delta_table(json_file_path, delta_table_path, schema)

    delta_table = DeltaTable(delta_table_path)
    history = delta_table.history()
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

    print(pd.DataFrame(history))
