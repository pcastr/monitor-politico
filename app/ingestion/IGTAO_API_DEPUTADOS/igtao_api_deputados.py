import json
import logging
import os
import sys
import time
from http import HTTPStatus
from pathlib import Path

import pandas as pd
import requests
from pandas import json_normalize

DEFAULT_TIMEOUT = 10
DEFAULT_RETRIES = 5
DEFAULT_BACKOFF_FACTOR = 1


def list_config_files() -> list:
    config_path = Path('config')
    json_files = [str(file) for file in config_path.glob('*.json')]
    if not json_files:
        raise FileNotFoundError(
            'Nenhum arquivo de configuração JSON foi encontrado na pasta.'
        )

    return json_files


def load_config_file(config_file: str) -> dict:
    """Carrega o arquivo de configuraçãõ JSON."""

    with open(config_file, 'r', encoding='utf-8') as file:
        config = json.load(file)
    return config


def validate_config(config: dict, log):
    """Valida a configuração carregada."""

    required_keys = [
        'table',
        'description',
        'full_table_name',
        'active',
        'endpoint',
    ]

    for key in required_keys:
        if key not in config:
            log.error(f'Configuração inválida: chave "{key}" ausente.')
            raise ValueError(f'Configuração inválida: chave "{key}" ausente.')


def build_url(
    config: dict, table_name: str, log, path_params=None, query_params=None
):
    base_url = config['endpoint'][0].get('base_url')
    url = config['endpoint'][0].get('url')

    full_url = f'{base_url}{url}'

    if path_params:
        try:
            full_url = full_url.format(**path_params)
        except KeyError as e:
            log.error('Chave de parâmetro ausente no path_params: %s', e)

    if query_params:
        query_string = []
        for key, value in query_params.items():
            processed_value = (
                ','.join(map(str, value)) if isinstance(value, list) else value
            )
            if processed_value:
                query_string.append(f'{key}={processed_value}')
        if query_string:
            full_url = f'{full_url}?{"&".join(query_string)}'

    return full_url


def get_url_with_query_params(config: dict, table_name: str, log):
    query_params = config['endpoint'][0].get('query_parameters', [])

    if not query_params:
        log.info('Nenhum parâmetro de consulta encontrado.')

    query_string = []
    for param in query_params:
        param_name = param.get('name')
        param_value = param.get('default')
        if param_value:
            query_string.append(f'{param_name}={param_value}')
            log.debug(
                f'{table_name} - '
                f'Parâmetro de consulta adicionado: '
                f'{param_name}={param_value}'
            )
        else:
            # log.warning(
            #     f'Tabela: {table_name} - '
            #     f'Parâmetro {param_name} não tem valor default.'
            # )
            pass

    query_params_dict = {
        param.get('name'): param.get('default')
        for param in query_params
        if param.get('default') is not None
    }

    url = build_url(
        config,
        table_name,
        log,
        query_params=query_params_dict if query_params_dict else None,
    )

    return url


def fetch_paginated_records(
    url,
    log,
    key_data: str,
    table_name: str,
    retries=DEFAULT_RETRIES,
    backoff_factor=DEFAULT_BACKOFF_FACTOR,
):
    """Realiza requisições paginadas a um endpoint."""
    all_records = []
    next_page = url

    while next_page:
        for attempt in range(retries):
            try:
                response = requests.get(next_page, timeout=DEFAULT_TIMEOUT)
                response.raise_for_status()
                data = response.json()

                records = data.get(key_data, [])
                all_records.extend(records)

                log.info(
                    f'{table_name} - Total de registros: %d',
                    len(all_records),
                )

                next_page_link = next(
                    (
                        link['href']
                        for link in data.get('links', [])
                        if link['rel'] == 'next'
                    ),
                    None,
                )

                if next_page_link:
                    next_page = next_page_link
                    log.info(
                        f'{table_name} - Página processada com sucesso: %s',
                        next_page,
                    )
                else:
                    next_page = None
                    log.info(f'{table_name} - Última página processada.')

                break

            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    sleep_time = backoff_factor * (2**attempt)
                    log.warning(
                        'Erro ao buscar registros: %s. Tentativa %d em %d...',
                        e,
                        attempt + 1,
                        retries,
                    )
                    time.sleep(sleep_time)
                else:
                    log.error(
                        'Erro ao buscar registros após %d tentativas: %s',
                        retries,
                        e,
                    )
                    raise

    return all_records


def get_deputados(
    log,
    config: dict,
    table_name: str,
    config_table_path: str,
    key_data='dados',
):
    log.debug(
        f'{table_name} - Chamando com os parâmetros:'
        f'config_file_path={config_table_path}'
    )

    url = get_url_with_query_params(config, table_name, log)

    try:
        records = fetch_paginated_records(
            url, log, key_data=key_data, table_name=table_name
        )
        log.info(f'{table_name} - Registros obtidos: {len(records)}')
        return records
    except Exception as e:
        log.error(f'{table_name} - Erro ao buscar registros: {e}')
        return None


def fetch_records_by_ids(
    config_data: dict,
    list_ids,
    table_name: str,
    log,
):
    all_records = []

    for id_value in list_ids:
        # Monte a URL para o ID específico
        # full_url = f'{base_url}{url_template}'.format(id=id_value)

        full_url = build_url(config_data, table_name, log)
        new_url = full_url.format(id=id_value)

        log.info(f'{table_name} - Buscando dados em: {new_url}')

        try:
            response = requests.get(new_url)
            if response.status_code == HTTPStatus.OK:
                data = response.json()
                all_records.append(data)
            else:
                log.error(
                    f'{table_name} - Falha ao puxar dados para ID -'
                    f'{id_value}: '
                    f'{response.status_code}'
                )
        except requests.RequestException as e:
            log.error(
                f'{table_name} - Erro ao puxar dados para ID {id_value}: {e}'
            )

    return all_records


def save_to_json(data, file_name, log):
    """Salva os dados em um arquivo JSON."""
    try:
        os.makedirs(
            '../../../data/deputados/PB/', exist_ok=True
        )  # Cria a pasta 'output' se não existir
        file_path = os.path.join('../../../data/deputados/PB/', file_name)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        log.info(f'Dados salvos em: {file_path}')
    except Exception as e:
        log.error(f'Erro ao salvar os dados em JSON: {e}')


def run_pipeline(config_table_path: str, log, cached_records=None):
    """Executa a pipeline com base no arquivo de configuração."""
    log.debug('Carregando configurações das tabelas: %s', config_table_path)

    try:
        config = load_config_file(config_table_path)
    except Exception as e:
        log.error(f'Erro ao carregar arquivo de configuração: {e}')
        return

    validate_config(config, log)

    table_name = config.get('table', 'Table não especificada')

    if config.get('active', True):
        records = cached_records

        if table_name == 'deputados':
            if not records:
                log.info(f'{table_name} - Extraindo dados...')
                records = get_deputados(
                    log, config, table_name, config_table_path
                )
                log.info(f'{table_name} - Dados carregados com sucesso.')
            else:
                log.info(
                    f'{table_name} - Usando dados previamente carregados.',
                )

            # print(json.dumps(records[0], indent=4, ensure_ascii=False))
            save_to_json(records, f'{table_name}.json', log)
            df_raw = pd.read_json('../../../data/deputados/PB/deputados.json')
            print(df_raw.head())

            return records

        if table_name == 'deputados_por_id':
            log.info(f'{table_name} - Extraindo dados...')

            list_ids = [deputado['id'] for deputado in records]
            log.info(f'{table_name} - Lista de IDs gerada: %s', list_ids)

            records_id = fetch_records_by_ids(
                config, list_ids, table_name, log
            )
            # print(json.dumps(records_id[0], indent=4, ensure_ascii=False))

            save_to_json(records_id, f'{table_name}.json', log)
            df_raw = pd.read_json(
                '../../../data/deputados/PB/deputados_por_id.json'
            )
            df_dados_normalizado = json_normalize(df_raw['dados'])

            print(df_dados_normalizado.head())

        else:
            log.warning(
                f'{table_name} - Tabela desconhecida ou não especificada.'
            )
            return None


def main():
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.DEBUG,
    )

    logging.getLogger('urllib3').propagate = False
    log = logging.getLogger('api-deputados')

    try:
        # Obter lista de arquivos de configuração
        config_files = list_config_files()
        log.info('Arquivos de configuração encontrados: %s', config_files)

        # Priorizar 'deputados.json' se estiver na lista
        if 'config/deputados.json' in config_files:
            config_files.remove('config/deputados.json')
            config_files.insert(0, 'config/deputados.json')

        # Cache para reutilizar registros carregados
        cached_records = None

        # Executar a pipeline para cada arquivo de configuração
        for config_file in config_files:
            cached_records = run_pipeline(config_file, log, cached_records)

    except FileNotFoundError as e:
        log.error(e)
        sys.exit(1)


if __name__ == '__main__':
    main()
