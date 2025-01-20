import asyncio
import json
import os

from app.ingestion.common.APIClient import APIClient


async def main():
    async with APIClient(
        base_url='https://dadosabertos.camara.leg.br'
    ) as client:
        try:
            # Faz a requisição à API
            response = await client.get_deputados(
                sigla_uf='PB', ordem='ASC', ordenarPor='nome'
            )

            # Define o caminho do arquivo
            file_path = 'data/deputados/PB/deputados_pb.json'

            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # Salva a resposta JSON no arquivo
            with open(file_path, 'w', encoding='utf-8') as file:
                json.dump(
                    response.model_dump(), file, ensure_ascii=False, indent=4
                )

            print(f"Dados salvos no arquivo '{file_path}' com sucesso!")

        except Exception as e:
            print(f'Ocorreu um erro: {e}')
