import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from http import HTTPStatus
from pathlib import Path

import requests
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import ArrayType, StructType

DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 4
DEFAULT_BACKOFF_FACTOR = 1


def setup_logging():
    log = logging.getLogger('api-deputados')
    log.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    )

    log.addHandler(handler)
    return log


def parse_argument() -> str:
    """Obtém o argumento passado via linha de comando."""
    print(f'Parâmetros: {sys.argv}')
    if len(sys.argv) > 1:
        return sys.argv[1]
    return None


def list_config_files(table_name: str) -> str:
    """Busca o JSON de configuração correspondente ao nome da tabela."""
    config_path = Path('config')
    json_files = [str(file) for file in config_path.glob(f'{table_name}.json')]

    if len(json_files) == 1:
        return json_files[0]

    return None


def load_config_file(config_file: str) -> dict:
    """Carrega o conteúdo de um arquivo JSON de configuração."""
    with open(config_file, 'r', encoding='utf-8') as file:
        return json.load(file)


def build_url(log, config: dict, path_params=None, extra_query_params=None):
    try:
        base_url = config['endpoint'][0]['base_url']
        url = config['endpoint'][0]['url'].format(**(path_params or {}))
        query_params_list = config['endpoint'][0].get('query_parameters', [])

        query_params = {
            param['name']: param.get('default', [])
            for param in query_params_list
        }

        formatted_query_params = {
            k: ','.join(map(str, v)) if isinstance(v, list) else v
            for k, v in query_params.items()
            if v not in [None, '', []]  # Ignora valores vazios
        }

        if extra_query_params:
            for k, v in extra_query_params.items():
                if v not in [None, '', []]:
                    if isinstance(v, list):
                        formatted_query_params[k] = ','.join(map(str, v))
                    else:
                        formatted_query_params[k] = v

        query_string = '&'.join(
            f'{k}={v}' for k, v in formatted_query_params.items()
        )

        full_url = f'{base_url}{url}'
        if query_string:
            full_url += f'?{query_string}'

        log.info(f'{config["table"]} - Url criada : {full_url}')
        return full_url
    except KeyError as e:
        if log:
            log.error('Erro na configuração do endpoint: %s', e)
        raise ValueError(f'Configuração inválida: {e}')


def fetch_records(
    url,
    log,
    headers=None,
    retries=DEFAULT_RETRIES,
    backoff_factor=DEFAULT_BACKOFF_FACTOR,
):
    """Busca registros de uma URL com retentativas e backoff exponencial."""
    for attempt in range(retries):
        try:
            response = requests.get(
                url, headers=headers
            )  # Corrigido headers=headers
            if response.status_code != HTTPStatus.OK:
                response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                sleep_time = backoff_factor * (2**attempt)
                log.warning(
                    f'Erro ao buscar registros: {e}. Tentando novamente em '
                    f'{sleep_time} segundos...'
                )
                log.warning(
                    f'Tentativa {attempt + 1} de {retries}.'
                )  # Corrigido o número da tentativa
                time.sleep(
                    sleep_time
                )  # Adicionada pausa para evitar requisições excessivas
            else:
                log.error(
                    f'Erro ao buscar registros após {retries} tentativas: {e}'
                )
                log.error(f'Falha na URL: {url}')
                raise e


def get_records_paginated(log, config_table: dict, max_pages=1000):
    """Coleta registros paginados de uma API."""
    all_records = []
    start_page = 1
    processes = 5
    empty_page_count = 0
    max_empty_pages = 3

    while empty_page_count < max_empty_pages and start_page <= max_pages:
        log.info(
            f'{config_table["table"]} - Extraindo dados da página {start_page}'
        )
        urls = []

        for _ in range(processes):
            extra_params = {'pagina': start_page}
            url = build_url(log, config_table, extra_query_params=extra_params)
            urls.append(url)
            start_page += 1  # Avança para a próxima página

        with ThreadPoolExecutor(max_workers=processes) as executor:
            futures = [
                executor.submit(fetch_records, url, log) for url in urls
            ]

            for future in as_completed(futures):
                try:
                    response = future.result()
                    result = response.json().get(config_table['key_data'], [])

                    if not result:  # Página vazia
                        empty_page_count += 1
                        log.debug(
                            f'{config_table["table"]} - Página vazia '
                            f'({empty_page_count}/{max_empty_pages})'
                        )
                    else:
                        empty_page_count = 0  # Reseta contador se houver dados
                        all_records.extend(result)
                        log.info(
                            f'{config_table["table"]} - Registros acumulados: '
                            f'{len(all_records)}'
                        )

                except Exception as exc:
                    log.error(
                        f'{config_table["table"]} - Erro ao processar página: '
                        f'{exc}'
                    )
                    raise exc

    log.info(
        f'{config_table["table"]} - Extração finalizada com '
        f'{len(all_records)} registros coletados.'
    )
    return all_records


def flatten_df(nested_df):
    while True:
        schema = nested_df.schema
        columns_to_explode = [
            column.name
            for column in schema
            if isinstance(column.dataType, ArrayType)
        ]

        columns_to_flatten = [
            column.name
            for column in schema
            if isinstance(column.dataType, StructType)
        ]

        if not columns_to_explode:
            break
        for column in columns_to_explode:
            nested_df = nested_df.withColumn(
                column, explode_outer(col(column))
            )

        for column in columns_to_flatten:
            for field in nested_df.schema[column].dataType.fields:
                nested_df = nested_df.withColumn(
                    f'{column}.{field.name}', col(f'{column}.{field.name}')
                )
                nested_df = nested_df.drop(column)

    return nested_df


def extract_table(config_table: dict, extraction_date: datetime, log):
    try:
        if config_table['endpoint'][0]['type'] == 'root':
            records = get_records_paginated(
                log,
                config_table,
            )

    except Exception as e:
        log.error(f'{config_table["table"]} - Erro ao buscar registros: {e}')

    log.info(f'{config_table["table"]} - Registros extraídos: {len(records)}')
    data = '\n'.join([json.dumps(row) for row in records])
    return data


def save_to_json(data, file_name, table_name, log):
    """Salva os dados em um arquivo JSON."""
    try:
        # Criar diretório, caso não exista
        os.makedirs('../../../data/deputados/PB/', exist_ok=True)
        file_path = os.path.join('../../../data/deputados/PB/', file_name)

        # Converter a string de dados para uma lista de objetos JSON
        records = [
            json.loads(line) for line in data.split('\n') if line.strip()
        ]

        # Salvar a lista no arquivo JSON
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=4)

        log.info(f'{table_name} - Dados salvos em: {file_path}')
    except Exception as e:
        log.error(f'{table_name} - Erro ao salvar os dados em JSON: {e}')


def run_pipeline(config_path: str, log):
    """Executa a pipeline baseada nas configurações carregadas."""
    log.debug('Carregando configurações da tabela %s', config_path)

    config_table = load_config_file(config_path)
    table_name = config_table.get('table')

    if config_table['active']:
        log.info(f'{table_name} - Iniciando processo de extração...')
        extraction_date = datetime.now() - timedelta(days=1)

        log.info(f'{table_name} - Data de extração: {extraction_date}')

        log.debug(f'{table_name} - Extraindo dados.')
        data = extract_table(config_table, extraction_date, log)

        file_name = (
            f'API_DEPT_{table_name}_'
            f'{extraction_date.strftime("%Y%m%d")}_'
            f'{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        )
        save_to_json(data, file_name, table_name, log)

    else:
        log.warning(
            'Tabela %s está inativa. Nenhuma extração será realizada.',
            table_name,
        )


def main():
    log = setup_logging()

    try:
        table = parse_argument()

        config_path = list_config_files(table)
        if not config_path:
            log.error(
                'Arquivo de configuração não encontrado para a tabela: %s',
                table,
            )
            sys.exit(1)

        run_pipeline(config_path, log)

    except Exception as e:
        log.exception('Erro inesperado: %s', e)
        sys.exit(1)


if __name__ == '__main__':
    main()
