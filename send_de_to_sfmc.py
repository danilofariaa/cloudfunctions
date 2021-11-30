import json
import requests
import time
import asyncio
import aiohttp
from google.cloud import bigquery
from asyncio import gather, run
from requests.exceptions import Timeout

from sfmc_to_bq import preparation


def get_data_secret(version):
    # Coleta Informacoes de credenciais para SFMC
    # Args:
    # Version : Versão da api do SFMC = atual v2
    if version == "v2":
        credentals = {
            "AUTH_URI_V2_RAIA": "https://mcjss9736km3nd134n6cv8-hcfy0.auth.marketingcloudapis.com/v2/token",
            "REST_URI_V2_RAIA": "https://mcjss9736km3nd134n6cv8-hcfy0.rest.marketingcloudapis.com/",
            "CLIENT_ID_V2_RAIA": "o09bav0efhzkub1fjtngg2kf",
            "CLIENT_SECRET_V2_RAIA": "QiWxOZR0fPB3yeJUbBQMxE6G",
        }
        return credentals
    else:
        return "version not supported"


def get_data_credentials(version):
    # Coleta as informacoes de data secret, como endpoint para autenticacao,consumo bem como client e client secret
    # Args:
    # Version(str) : Versão da API do SFMC

    c = get_data_secret(version)
    if version == "v2":
        credendials = {
            "client_id": c["CLIENT_ID_V2_RAIA"],
            "client_secret": c["CLIENT_SECRET_V2_RAIA"],
            "grant_type": "client_credentials",
        }
        return credendials
    else:
        return "version not supported"


def get_credendials(version):
    # Pega as credenciais do usuario para logar na API do SFMC
    # Args:
    # Version(str) : Versão da API do SFMC
    credentials = get_data_credentials(version)
    return credentials


def check_status_code(status):
    # Verifica o status code de uma request (pode ser chamada mais de uma vez)
    # Args:
    # status : Status code of a request

    if status >= 200 and status <= 299:
        return "success", status
    elif status >= 300 and status <= 399:
        return "redirect", status
    elif status >= 400 and status <= 499:
        return "error_client", status
    elif status >= 500 and status <= 599:
        return "error_server", status


def response_data(url, payload):
    # Faz uma requisicao para API do SFMC para atuenticacao
    # Retorna um dic com Token, Tipo do Token, e Tempo de expiracao
    # Args:
    # Url : Url da Request,
    # Payload: Dados da Request

    response = requests.post(url=url, data=payload)
    check_status, status = check_status_code(response.status_code)
    body = json.loads(response.content)
    if check_status == "success":
        print("Autenticado")
        auth_response = {
            "token": body["access_token"],
            "token_type": body["token_type"],
            "expiration": body["expires_in"],
        }
        return str(auth_response["token_type"]) + " " + str(auth_response["token"])
    else:
        return status


def get_url_auth_dotenv(version):
    # Pega dados de auth do .env
    # Args:
    #   Version(str) : Versão da API

    auth = get_data_secret(version)
    if version == "v1":
        url = auth["AUTH_URI_V1_RAIA"]
        return url
    elif version == "v2":
        url = auth["AUTH_URI_V2_RAIA"]
        return url
    else:
        return "version not supported"


def get_auth(payload, version):
    # Realiza a autenticacao
    # Args:
    #   Payload: Authentication data
    #   API Version: Version API

    url = get_url_auth_dotenv(version)
    authentication = response_data(url, payload)
    return str(authentication)

def query():
    
    """
    Função que realiza query no GCP 
    Args: df (dataframe pandas)
    """
    print("Consultando dados...")
    client = bigquery.Client()
    query = "SELECT golden_id from `data_adsync.drogaraia_app_conversion_off_all` "
    query_job = client.query(query).to_dataframe()  # request for API

    return query_job


def df_dict(df):
    """
    Função que transforma o dataframe em dicionario
    Args: df (dataframe pandas)
    """
    lst = []
    for d in df:
        lst.append(d.to_dict("records"))
    return lst


def split_dataframe(dataframe):
    """
    Função que splita o dataframe em pedaços de 2500 unidades (padrão para envio ao SFMC)
    Args:  dataframe (dataframe pandas com os registros vindos da query no bq do GCP)
    """
    print("Split do dataframe...")
    size = 2500
    list_of_dfs = [
        dataframe.loc[i : i + size - 1, :] for i in range(0, len(dataframe), size)
    ]
    return list_of_dfs



def send_de_to_sfmc(payload, d):
    """
    Função que retorna os dados do GCP para Data Extension
    Args:  
        payload (autenticação) 
        d (dicionario de dados com os dados)

    """
    print("Enviando dados para SFMC")
    url = "https://mcjss9736km3nd134n6cv8-hcfy0.rest.marketingcloudapis.com/data/v1/async/dataextensions/key:307DBAE2-93D3-4CC9-BB5E-BB01F76028E2/rows"
    headers = {"Authorization": payload}
    to_json = json.dumps(d)
    response = requests.post(url=url, data=to_json, headers=headers)
    # check_status = check_status_code(int(response.status_code))
    body = json.loads(response.content)
    if response.status_code >= 200 and response.status_code <= 299:
        print(body)
    else:
        print(body)
        print("deu ruim", response.status_code)


if __name__ == "__main__":
    # API Version V2 uses OAuth2.0
    # Dicionario de dados com configurações iniciais : Versão API, Bandeira, Maximo de rodadas
    cfg_initial = {"version": "v2"}
    # Busca as credenciais para autenticar na API
    credentials = get_credendials(cfg_initial["version"])
    # Autenticação na SFMC API
    auth = get_auth(credentials, cfg_initial["version"])

    bases = {
        "drogasil": "data_adsync.drogasil_app_conversion_off_all",
        "drogaraia": "data_adsync.drogaraia_app_conversion_off_all",
        "de_drogasil": "193EF13C-374C-4668-BF98-9FDAE7E337FB",
        "de_raia": "307DBAE2-93D3-4CC9-BB5E-BB01F76028E2",
    }
    # Query para buscar dados numa tabela do BQ
    query_start = time.time()
    df = query()
    query_finish = time.time() - query_start
    print(f" Query Excution Time: {query_finish}")
    # Split do dataframe em fatias de 2500 registros
    df_splited = split_dataframe(df)
    # Quantidade de slots de 2500 para SFMC
    length = len(df_splited)
    print(length)
    # Transformacao de dataframe para dicionario
    dict_list = df_dict(df_splited)
    # Loop para inserir dados no SFMC
    start_consume_sfmc = time.time()
    stop_steps = [300, 600]
    for i in range(0, length - 1):
        print(f"round : {i+1}")
        if i in stop_steps:
            print("Renovando Credenciais..")
            auth = get_auth(credentials, cfg_initial["version"])
        d = {"items": dict_list[i]}
        send_de_to_sfmc(auth, d)

    end_consume_sfmc = time.time() - start_consume_sfmc
    print(f" SFMC Excution Time: {end_consume_sfmc}")
