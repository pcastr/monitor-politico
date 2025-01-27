import json
import logging
import time
from http import HTTPStatus
from pathlib import Path

import requests

DEFAULT_TIMEOUT = 10
DEFAULT_RETRIES = 5
DEFAULT_BACKOFF_FACTOR = 1


def list_config_files() -> list:
    """Lista todos os arquivos JSON no diretório de configuração."""
    config_path = Path('config')
    json_files = [str(file) for file in config_path.glob('*.json')]

    if not json_files:
        raise FileNotFoundError(
            'Nenhum arquivo de configuração JSON foi encontrado na pasta config.'
        )

    return json_files


def load_config_file(config_file: str) -> dict:
    """Carrega o arquivo de configuração JSON."""
    with open(config_file, 'r', encoding='utf-8') as file:
        config_data = json.load(file)
    return config_data


def validate_config(config_data: dict, log):
    """Valida a configuração carregada."""
    required_keys = [
        'table',
        'description',
        'full_table_name',
        'active',
        'endpoint',
    ]
    for key in required_keys:
        if key not in config_data:
            log.error(f'Configuração inválida: chave "{key}" ausente.')
            raise ValueError(f'Configuração inválida: chave "{key}" ausente.')


def fetch_paginated_records(
    url,
    log,
    key_data: str,
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
                    'Total de registros acumulados até agora: %d',
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
                    log.info('Página processada com sucesso: %s', next_page)
                else:
                    next_page = None  # Não há mais páginas
                    log.info('Última página processada.')

                break  # Sai do loop de tentativas se tudo funcionar

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


def fetch_records_by_ids(
    config_data: dict,
    list_ids,
    table_name: str,
    log,
):
    all_records = []  # Lista para armazenar todos os JSONs retornados

    for id_value in list_ids:
        # Monte a URL para o ID específico
        # full_url = f'{base_url}{url_template}'.format(id=id_value)

        full_url = build_url(config_data, table_name, log)
        new_url = full_url.format(id=id_value)

        log.info(f'Fetching data from URL: {new_url}')

        try:
            response = requests.get(new_url)
            if response.status_code == HTTPStatus.OK:
                data = response.json()
                all_records.append(data)
            else:
                log.error(
                    f'Failed to fetch data for ID {id_value}: '
                    f'{response.status_code}'
                )
        except requests.RequestException as e:
            log.error(f'Error while fetching data for ID {id_value}: {e}')

    return all_records


def build_url(
    config_data: dict,
    table_name: str,
    log,
    path_params=None,
    query_params=None,
):
    """
    Constrói a URL completa para a chamada da API.

    Args:
        config_data (dict): Dados de configuração contendo base_url e url.
        table_name (str): Nome da tabela para log.
        log (obj): Objeto de log para registrar informações.
        path_params (dict, opcional): Parâmetros a serem substituídos no path.
        query_params (dict, opcional): Parâmetros de consulta (query string).

    Returns:
        str: URL completa.
    """
    base_url = config_data['endpoint'][0].get('base_url')
    url = config_data['endpoint'][0].get('url')

    # Constrói a URL inicial com base_url e url
    full_url = f'{base_url}{url}'

    # Substituindo parâmetros no path da URL
    if path_params:
        try:
            full_url = full_url.format(**path_params)
        except KeyError as e:
            log.error('Chave de parâmetro ausente no path_params: %s', e)
            raise ValueError(f'Parâmetro ausente para substituição: {e}')

    # Adicionando parâmetros de consulta (query string)
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

    log.info('Url de requisição da tabela %s: %s', table_name, full_url)
    return full_url


def get_url_with_query_params(config_data: dict, table_name: str, log):
    query_params = config_data['endpoint'][0].get('query_parameters', [])

    if not query_params:
        log.info('Nenhum parâmetro de consulta encontrado.')

    query_string = []
    for param in query_params:
        param_name = param.get('name')
        param_value = param.get('default')  # Usando o valor padrão
        if param_value:
            query_string.append(f'{param_name}={param_value}')
            log.debug(
                f'Tabela: {table_name} - '
                f'Parâmetro de consulta adicionado: '
                f'{param_name}={param_value}'
            )
        else:
            log.warning(
                f'Tabela: {table_name} - '
                f'Parâmetro {param_name} não tem valor default.'
            )

    # Construção do dicionário de parâmetros
    query_params_dict = {
        param.get('name'): param.get('default')
        for param in query_params
        if param.get('default') is not None
    }

    # Log da URL construída
    url = build_url(
        config_data,
        table_name,
        log,
        query_params=query_params_dict if query_params_dict else None,
    )
    log.debug(f'URL construída: {url}')
    return url


def get_deputados(
    log,
    config_data: dict,
    table_name: str,
    config_table_path: str,
    key_data='dados',
):
    """
    Busca informações sobre deputados, incluindo suporte à paginação.

    Args:
        config_data (dict): Dados de configuração.
        table_name (str): Nome da tabela ou endpoint.

    Returns:
        list: Lista de registros coletados.
    """
    log.debug(
        f'Chamando get_deputados com os parâmetros:'
        f'config_file_path={config_table_path}, table_name={table_name}'
    )

    url = get_url_with_query_params(config_data, table_name, log)

    try:
        records = fetch_paginated_records(url, log, key_data=key_data)
        log.info(f'Registros obtidos com sucesso: {len(records)}')
        return records
    except Exception as e:
        log.error(f'Erro ao buscar registros: {e}')
        return None


def main():
    config_table_path = list_config_files()

    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.DEBUG,
    )
    log = logging.getLogger('api-deputados')

    for file in config_table_path:
        config = load_config_file(file)
        table_name = config.get('table', 'Tabela não especificada')

        # if table_name == 'deputados':
        #     log.info('Extraindo dados da tabela %s', table_name)
        #     records = get_deputados(
        #         log,
        #         config,
        #         table_name='deputados',
        #         config_table_path=config_table_path,
        #     )
        #
        #     if records:
        #         print(records)
        #
        #     # TODO: Salvar os registros em um DataFrame ou em outro lugar
        #     else:
        #         log.warning('Nenhum registro encontrado.')

        if table_name == 'deputados_por_id':
            log.info('Extraindo dados da tabela %s', table_name)
            deputado_config = load_config_file('config/deputados.json')
            records = get_deputados(
                log,
                deputado_config,
                table_name='deputados',
                config_table_path='config/deputados.json',
            )

            # print(records)

            list_ids = []

            for deputado in records:
                list_ids.append(deputado['id'])

            print(f'LISTA DE ID: \n{list_ids}')
            deputados_por_id_records = fetch_records_by_ids(
                config,
                list_ids,
                table_name,
                log,
            )

            print(deputados_por_id_records)


if __name__ == '__main__':
    main()


# def fetch_records_by_ids(base_url, url_template, ids, log):
#     all_records = []  # Lista para armazenar todos os JSONs retornados
#
#     for id_value in ids:
#         # Monte a URL para o ID específico
#         full_url = f'{base_url}{url_template}'.format(id=id_value)
#
#         log.info(f'Fetching data from URL: {full_url}')
#
#         try:
#             response = requests.get(full_url)
#             if response.status_code == 200:
#                 data = response.json()  # Parse o JSON da resposta
#                 all_records.append(data)  # Adicione o JSON à lista
#             else:
#                 log.error(
#                     f'Failed to fetch data for ID {id_value}: {response.status_code}'
#                 )
#         except requests.RequestException as e:
#             log.error(f'Error while fetching data for ID {id_value}: {e}')
#
#     return all_records
#
#
# # Configuração de exemplo
# base_url = 'https://dadosabertos.camara.leg.br'
# url_template = '/api/v2/deputados/{id}'  # Exemplo de URL RESTful
# list_id = list(range(1, 10))  # Exemplo: IDs de 1 a 9
#
# # Configuração de logging
# logging.basicConfig(
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     level=logging.INFO,
# )
# log = logging.getLogger('api-deputados')
#
# # Chamar a função
# records = fetch_records_by_ids(base_url, url_template, list_id, log)
#
# # Imprimir ou processar os dados consolidados
# print(f'Total de registros coletados: {len(records)}')
# print(records)  # Exibe a lista consolidada de JSONs
