import asyncio
import logging

from aiohttp import ClientResponseError, ClientSession
from pydantic import ValidationError

from ..IGTAO_API_DEPUTADOS.models.model_deputados import DeputadosResponse
from ..IGTAO_API_DEPUTADOS.models.model_deputados_id import DeputadosIdResponse

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class APIClient:
    def __init__(self, base_url):
        self.base_url = base_url
        self.session = None

    def build_url(self, endpoint, path_params=None, query_params=None):
        """
        Constrói a URL completa para a chamada da API.

        Args:
            endpoint (str): Endpoint da API.
            path_params (dict, opcional): Parâmetros a serem substituídos
                no endpoint.
            query_params (dict, opcional): Parâmetros de consulta
                (query string).

        Returns:
            str: URL completa.

        """
        url = f'{self.base_url}{endpoint}'
        if path_params:
            for key, value in path_params.items():
                url = url.replace(f'{{{key}}}', str(value))
        if query_params:
            query_string = '&'.join(
                f'{key}={value}' for key, value in query_params.items()
            )
            url = f'{url}?{query_string}'
        return url

    async def __aenter__(self):
        self.session = ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self.session:
            await self.session.close()

    async def fetch_data(self, url, model=None):
        """
        Busca dados da API e valida com o modelo Pydantic, se fornecido.

        Args:
            url (str): URL completa para a requisição.
            model (BaseModel, opcional): Modelo Pydantic para
                validação da resposta.

        Returns:
            dict | BaseModel | None: Dados validados ou resposta bruta.
        """
        try:
            logger.info(f'Fetching data from URL: {url}')
            async with self.session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                if model:
                    try:
                        return model(**data)
                    except ValidationError as e:
                        logger.error(f'Erro de validação: {e}')
                        return None
                return data
        except ClientResponseError as e:
            logger.error(f'Erro ao obter dados da URL {url}: {e}')
            return None

    async def fetch_in_batches(self, fetch_func, items, batch_size=10):
        """
        Busca dados em lotes para melhorar a eficiência.

        Args:
            fetch_func (coroutine): Função de busca para cada item.
            items (list): Lista de itens a serem processados.
            batch_size (int): Tamanho do lote para processamento.

        Returns:
            list: Resultados da busca em todos os lotes.
        """
        if not items:
            logger.warning('Nenhum item fornecido para processamento.')
            return []

        results = []
        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]
            logger.info(f'Processando lote de {len(batch)} itens.')
            tasks = [fetch_func(item) for item in batch]
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)
        return results

    async def get_deputados(
        self, sigla_uf=None, ordem='ASC', ordenarPor='nome', itens=None
    ):
        """
        Busca informações sobre deputados.

        Args:
            sigla_uf (str, opcional): UF do deputado. Exemplo: "SP".
            ordem (str, opcional): Ordem dos resultados ("ASC" ou "DESC").
            ordenarPor (str, opcional): Campo para ordenar. Exemplo: "nome".

        Returns:
            DeputadosResponse | None: Dados validados ou None em caso de erro.
        """
        query_params = {
            'siglaUf': sigla_uf,
            'ordem': ordem,
            'ordenarPor': ordenarPor,
            'itens': itens,
        }
        query_params = {
            key: value
            for key, value in query_params.items()
            if value is not None
        }
        url = self.build_url(
            endpoint='/api/v2/deputados',
            query_params=query_params if query_params else None,
        )
        return await self.fetch_data(url, model=DeputadosResponse)

    async def get_deputados_by_id(self, id_deputado):
        url = self.build_url(
            endpoint=f'/api/v2/deputados/{id_deputado}',
        )
        return await self.fetch_data(url, model=DeputadosIdResponse)

    async def get_despesas_deputado(self, id_deputado, ano=None, mes=None):
        """
        Busca as despesas de um deputado específico.

        Args:
            id_deputado (int): ID do deputado.
            ano (int, opcional): Ano para filtrar as despesas.
            mes (int, opcional): Mês para filtrar as despesas.

        Returns:
            dict | None: Dados das despesas ou None em caso de erro.
        """
        query_params = {'ano': ano, 'mes': mes}

        url = self.build_url(
            endpoint=f'/api/v2/deputados/{id_deputado}/despesas',
            query_params=query_params if query_params else None,
        )
        return await self.fetch_data(url)
