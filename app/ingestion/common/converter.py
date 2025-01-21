import logging
from datetime import date, datetime
from typing import Any, List, Type, Union

import pyarrow as pa
from pydantic import BaseModel

# Configuração básica do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PydanticToPyArrowConverter:
    """
    Converte modelos Pydantic em tipos e esquemas PyArrow.
    Suporta diversos tipos de dados, incluindo tipos nativos do Python,
    listas, tipos opcionais e tipos de data (datetime e date).
    """

    @staticmethod
    def py_type_to_arrow_type(py_type: Any) -> pa.DataType:  # noqa: PLR0911
        """
        Converte tipos nativos de Python para tipos PyArrow.

        Suporta:
            - int
            - float
            - str
            - bool
            - List
            - Optional
            - datetime, date
            - Outros tipos podem ser adicionados conforme necessário.

        Args:
            py_type: O tipo Python a ser convertido.

        Returns:
            pa.DataType: Tipo PyArrow correspondente.

        Raises:
            TypeError: Caso o tipo não seja suportado.
        """
        # Mapeamento de tipos básicos
        if py_type is int:
            return pa.int64()
        elif py_type is float:
            return pa.float64()
        elif py_type is str:
            return pa.string()
        elif py_type is bool:
            return pa.bool_()

        # Suporte para datetime
        elif py_type is datetime:
            return pa.timestamp(
                's'
            )  # ou 'ms' para milissegundos, conforme necessário
        # Suporte para date
        elif py_type is date:
            return pa.date32()

        # Suporte para listas (List)
        elif hasattr(py_type, '__origin__') and py_type.__origin__ is list:
            item_type = py_type.__args__[0]
            return pa.list_(
                PydanticToPyArrowConverter.py_type_to_arrow_type(item_type)
            )

        # Suporte para tipos opcionais (Optional)
        elif hasattr(py_type, '__origin__') and py_type.__origin__ is Union:
            # Verifica se é Optional, que é equivalente a Union[NoneType, T]
            args = py_type.__args__
            if type(None) in args:
                non_none_type = [t for t in args if t is not type(None)][0]
                return PydanticToPyArrowConverter.py_type_to_arrow_type(
                    non_none_type
                )

        raise TypeError(
            f'Tipo {py_type} não suportado para conversão para PyArrow.'
        )

    @staticmethod
    def pydantic_to_field(name: str, field_type: Any) -> pa.Field:
        """
        Converte um campo de um modelo Pydantic para um campo PyArrow.

        Args:
            name: Nome do campo.
            field_type: Tipo do campo.

        Returns:
            pa.Field: Campo PyArrow correspondente.
        """
        try:
            arrow_type = PydanticToPyArrowConverter.py_type_to_arrow_type(
                field_type
            )
            return pa.field(name, arrow_type)
        except TypeError as e:
            logger.error(f"Erro ao converter o campo '{name}': {e}")
            raise

    @staticmethod
    def pydantic_model_to_fields(model: Type[BaseModel]) -> List[pa.Field]:
        """
        Converte todos os campos de um modelo Pydantic em uma lista de campos
        PyArrow.

        Args:
            model: A classe de modelo Pydantic.

        Returns:
            List[pa.Field]: Lista de campos PyArrow correspondentes.
        """
        fields = []
        for name, annotation in model.__annotations__.items():
            try:
                field = PydanticToPyArrowConverter.pydantic_to_field(
                    name, annotation
                )
                fields.append(field)
            except Exception as e:
                logger.warning(f"Falha ao processar o campo '{name}': {e}")
        return fields

    @staticmethod
    def pydantic_to_schema(model: Type[BaseModel]) -> pa.Schema:
        """
        Converte um modelo Pydantic para um esquema PyArrow.

        Args:
            model: A classe de modelo Pydantic.

        Returns:
            pa.Schema: O schema correspondente em PyArrow.
        """
        try:
            fields = PydanticToPyArrowConverter.pydantic_model_to_fields(model)
            return pa.schema(fields)
        except Exception as e:
            logger.error(
                f"Erro ao criar o schema para o modelo '{model.__name__}': {e}"
            )
            raise
