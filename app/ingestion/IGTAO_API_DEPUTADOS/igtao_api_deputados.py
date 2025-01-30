import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests

DEFAULT_TIMEOUT = 9
DEFAULT_RETRIES = 4
DEFAULT_BACKOFF_FACTOR = 0


def setup_logging():
    """Configura o sistema de logging."""
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
        query_params = config['endpoint'][0].get('query_parameters', {})

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


def fetch_records_paginated(log, config_table: dict):
    all_records = []
    table_name = config_table.get('table')
    source_fields = [f['source'].strip() for f in config_table['fields']]

    pagina = 1
    itens = 1000

    while True:
        extra_params = {
            'itens': itens,
            'pagina': pagina,
        }
        url = build_url(log, config_table, extra_query_params=extra_params)

        for attempt in range(DEFAULT_RETRIES):
            try:
                response = requests.get(url, timeout=DEFAULT_TIMEOUT)
                response.raise_for_status()
                response_data = response.json()

                records = response_data.get(config_table['key_data'], [])
                all_records.extend(records)

                log.info(
                    f'{table_name} - Total de registros: %d',
                    len(all_records),
                )

                if len(records) < itens:
                    log.info(f'{table_name} - Última página alcançada.')
                    if isinstance(all_records, list):
                        filtered_records = [
                            {
                                key: item.get(key)
                                for key in source_fields
                                if key in item
                            }
                            for item in all_records
                        ]
                    elif isinstance(all_records, dict):
                        filtered_records = {
                            key: all_records.get(key)
                            for key in source_fields
                            if key in all_records
                        }
                    else:
                        filtered_records = all_records

                    return filtered_records

                # Caso contrário, ir para a próxima página
                pagina += 1
                break  # Para tentar novamente no caso de erro de requisição

            except requests.exceptions.RequestException as e:
                log.error('Erro na requisição: %s', e)
                break


def extract_table(config_table: dict, extraction_date: datetime, log):
    try:
        records = fetch_records_paginated(
            log,
            config_table,
        )

    except Exception as e:
        log.error(f'{config_table["table"]} - Erro ao buscar registros: {e}')

    log.info(f'{config_table["table"]} - Registros extraídos: {len(records)}')
    data = '\n'.join([json.dumps(row) for row in records])
    return data


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

        # TODO: adicionar upload_to_blob(data, blob_name, log)

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
