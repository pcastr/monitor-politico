from typing import List, Optional
from pydantic import BaseModel, Field, EmailStr


class Dados(BaseModel):
    id: int
    uri: str
    nome: str
    siglaPartido: str
    uriPartido: str
    siglaUf: str
    idLegislatura: int
    urlFoto: str
    email: str


class Links(BaseModel):
    href: str
    rel: str
    type_: Optional[str] = Field(None, alias='type')


class DeputadosResponse(BaseModel):
    dados: List[Dados]
    links: List[Links]
