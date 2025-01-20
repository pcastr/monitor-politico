from typing import List, Optional
from pydantic import BaseModel, EmailStr


class Gabinete(BaseModel):
    andar: str
    email: Optional[EmailStr]
    nome: Optional[str]
    predio: Optional[str]
    sala: Optional[str]
    telefone: Optional[str]


class UltimoStatus(BaseModel):
    condicaoEleitoral: Optional[str]
    data: Optional[str]
    descricaoStatus: Optional[str]
    email: Optional[EmailStr]
    gabinete: Optional[Gabinete]
    id: int
    idLegislatura: int
    nome: Optional[str]
    nomeEleitoral: Optional[str]
    siglaPartido: Optional[str]
    siglaUf: Optional[str]
    situacao: Optional[str]
    uri: Optional[str]
    uriPartido: Optional[str]
    urlFoto: Optional[str]


class Dados(BaseModel):
    cpf: Optional[str]
    dataFalecimento: Optional[str]
    dataNascimento: Optional[str]
    escolaridade: Optional[str]
    id: int
    municipioNascimento: Optional[str]
    nomeCivil: Optional[str]
    redeSocial: Optional[List[str]]
    sexo: Optional[str]
    ufNascimento: Optional[str]
    ultimoStatus: Optional[UltimoStatus]
    uri: Optional[str]
    urlWebsite: Optional[str]


class Links(BaseModel):
    href: str
    rel: str


class DeputadosIdResponse(BaseModel):
    dados: Dados
    links: List[Links]
