import json
import requests
import time
import asyncio
import pandas as pd
import google
import aiohttp
from asyncio import gather, run
from google.cloud import storage, bigquery
from datetime import datetime

from requests.exceptions import Timeout


def get_data_secret(version):
    # Coleta Informacoes de credenciais para SFMC
    # Args:
    # Version : Versão da api do SFMC = atual v2
    if version == "v2":
        credentals = {
            "AUTH_URI_V2_RAIA": "https://mcjss9736km3nd134n6cv8-hcfy0.auth.marketingcloudapis.com/v2/token",
            "REST_URI_V2_RAIA": "https://mcjss9736km3nd134n6cv8-hcfy0.rest.marketingcloudapis.com/",
            "CLIENT_ID_V2_RAIA": "",
            "CLIENT_SECRET_V2_RAIA": "",
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


def unpack(list):

    # Desempacota os dados piis vindos da requisicao
    # Args
    # list:  lista de dados da requisicao  (vindas do request.content)
    lst = []
    for item in list:
        for key, value in item.items():
            if key == "items":
                lst.append(value)
    return lst


async def create_dataframe(data, de_key, de_fields):
    # Cria um dataframe e preenche dados NaN como NULL
    # Args
    # Data : dict com os dados piis vindos da SFMC

    df = pd.DataFrame(data, columns=de_fields[de_key])
    df.fillna("NULL", inplace=True)

    return df


async def csv_to_gcs(dataframe, data_extension: str, config, round):
    # Args
    # dataframe : dict with SFMC
    # brand : Raia or Drogasil
    # config : Project initial configs such as (project, bucketname)
    # Save file on GCS bucket (adsync)
    date = datetime.today().strftime("%Y-%m-%d")
    filepath = f"{data_extension}_pii_{date}_{round}.csv"
    uri = await save_file_on_bucket(
        dataframe.to_csv(index=False),
        config["project"],
        config["bucketname"][0],
        filepath,
    )
    return uri


async def save_file_on_bucket(
    content: bytes,
    project: str,
    bucketname: list,
    filepath: str,
    content_type="text/plain",
):
    """
    Salva o csv com piis no bucket do GCS
    Args = content
           project : arquivo csv
           bucketname : nome do bucket GCS
           filepath : caminho salvo no GCS
           content-type : tipo de conteudo
    """

    storage_client = storage.Client(project)
    uri = f"gs://{bucketname}/{filepath}"

    print(f"{filepath} Saving in Bucket...")

    try:
        bucket = storage_client.bucket(bucketname)
        blob = bucket.blob(filepath)
    except google.cloud.exceptions.NotFound:
        print(f"[ERROR] Storage Failure. " f"Bucket gs://{bucketname} not Found.")
        return

    blob.upload_from_string(content, content_type=content_type)
    print(f"{uri} file loaded successfully!")
    return uri


async def create_schema(df):
    # Cria schema para uma tabela do BQ
    # Args
    #   df: Dataframe pandas criado para os dados piis
    lst = df.columns.tolist()
    schema = []
    types = {"int": "INTEGER", "float": "FLOAT", "bool": "BOOLEAN", "str": "STRING"}
    for key in lst:
        t = str(type(key).__name__)
        if t in types:
            typ = types[t]
            schema.append(bigquery.SchemaField(key, typ))

    return schema


async def gcs_to_bq(project, dataset_name, data_extension, schema, gcs_uri):
    """
    Salva o arquivo csv com dados piis no Big Query
    Args: project
          dataset_name = nome do dataset
          brand = bandeira (drogasil ou raia)
          schema = schema da tabela
          gcs_uri = uri do arquivo csv no gcs
    """
    print("Inicializando Client .. ")
    bq_client = bigquery.Client(project)
    dataset_ref = bq_client.get_dataset(project + "." + dataset_name)
    # table_name = f"{data_extension}_{datetime.today().strftime('%Y-%m-%d')}"
    table_name = "GOLDEN_ID_HASH_2021-10-21"
    print(f"Reporting  \n {dataset_name} => {table_name}  \n  {schema} \n {gcs_uri}")
    print("configurando job ... ")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = bq_client.load_table_from_uri(
        gcs_uri, dataset_ref.table(table_name), job_config=job_config
    )

    print("Executando job {}...".format(load_job.job_id))
    load_job.result()  # Waits for table load to complete.

    destination_table = bq_client.get_table(dataset_ref.table(table_name))
    print(f"[{table_name}] Sucesso no carregamento da tabela!")


async def clean_pii(dados_pii):
    """
    Para cada requisicao, pega apenas os dados da chave items vindas da request
    Args:
        dados_pii: lista de requests
    """
    lst = []
    count = 0
    for d in dados_pii:
        for i in d:
            if i != None:
                for key, value in i.items():
                    if key == "items":
                        count += 1
                        lst.append(value)
                    else:
                        continue

    print(f"total de items {count}")
    return lst


async def merge_pii(dp_clean):

    # Lista com dicionarios com todas as informacoes completas juntas
    # Args :
    # dp_clean : Dicionario com dados piis tratados

    lst = []
    for i in dp_clean:
        for j in i:
            a = {**j["keys"], **j["values"]}
            lst.append(a)
    return lst


def extract_keys(data_extension):

    # Carrega as chaves da data extensions para formacao de nome de tabela
    # Args:
    # data_extension nome das chaves do dicionario de data extensions

    lst = []
    de = data_extension.keys()
    for i in de:
        lst.append(i)
    return lst


async def none_check_list(list):
    # Checa se uma lista esta vazia
    # Args:
    # lista
    lst = []
    for val in list:
        if val != None:
            lst.append(val)
    return lst


async def get_pii(session, url, token, number):

    # Realiza request assincrona para SFMC
    # Args:
    # session: session aiohttp
    # url: url de acesso
    # token : token de acesso para request sfmc
    # number : rodada de iteracao do loop de request
    cont = 0
    headers = {"Authorization": token}
    try:
        async with session.get(url=url, headers=headers, timeout=None) as resp:
            print(f"Response : {number} ")
            data = await resp.json()
            cont += 1
            return data
    except (
        aiohttp.ClientConnectionError,
        aiohttp.ClientPayloadError,
        aiohttp.ServerDisconnectedError,
        asyncio.IncompleteReadError,
    ) as s:
        print(s)


async def get_piis(
    start, stop, token, externalkey
):  # task para retornar 750k de registros
    tasks = []
    async with aiohttp.ClientSession() as session:
        print(f"Collecting 750k of Piis data ")
        for number in range(start, stop):
            print(f"Request number : {number}")
            url = "https://mcjss9736km3nd134n6cv8-hcfy0.rest.marketingcloudapis.com/data/v1/customobjectdata/key/{externalkey}/rowset?$pageSize=2500&$page={number}"
            tasks.append(
                asyncio.ensure_future(
                    get_pii(
                        session,
                        url.format(externalkey=externalkey, number=number),
                        token,
                        number,
                    )
                )
            )
            await asyncio.sleep(0.27)
        piis_data = await asyncio.gather(*tasks)
        lst = []
        lst.append(piis_data)
    return lst




async def preparation(
    max_round,
    token,
    externalkey,
    data_extension_keys,
    data_extension_fields,
    config,
):
    """
    Task assincrona para retornar 750.000 registros por rodada

    Args:
        max_round  = quantidade de rodadas
        token = token sfmc de acesso
        external_key = external keuy
        data_extensions_keys
        data_extension_fields
        config
    """

    round = 1
    start = 1

    stop_steps = [
        6,
        12,
        18,
        24,
        30,
        36,
        42,
        48,
        54,
        60,
        66,
        72,
        78,
        84,
        90,
        96,
        102,
        108,
    ]
    while round < max_round:
        if round in stop_steps:
            print("Time get some new token ... ")
            # Busca as credenciais para autenticar na API
            credentials = get_credendials(cfg_initial["version"])
            # Autenticação na SFMC API
            token = get_auth(credentials, cfg_initial["version"])

            print("waiting 1.5 secs")
            await asyncio.sleep(1.5)
        stop = (round * 300) + 1
        print(f"Calling {round} Round")
        res = await get_piis(start, stop, token, externalkey)
        none_check = await none_check_list(res)
        dp_clean = await clean_pii(none_check)
        merged_data = await merge_pii(dp_clean)
        # filtered_data = await filter_data(query_data, merged_data)
        df = await create_dataframe(
            dp_clean, data_extension_keys, data_extension_fields
        )
        print(df)
        uri = await csv_to_gcs(df, data_extension_keys, config, round)
        schema = await create_schema(df)
        await gcs_to_bq(
            config["project"], "data_adsync", data_extension_keys, schema, uri
        )

        start = stop
        round += 1


if __name__ == "__main__":
    # API Version V2 uses OAuth2.0
    # Dicionario de dados com configurações iniciais : Versão API, Bandeira, Maximo de rodadas = total de registros / 750.000
    cfg_initial = {
        "version": "v2",
        "brand": ["raia", "drogasil"],
        "round_max": 107,
    }

    # Dicionario com as data extensions e external keys
    data_extension = {
        "MCV_LGPD_RAIA": "50594300-03D9-485D-9E4D-39CB331023B6",
        "MCV_LGPD_DROGASIL": "C1E4F6ED-DFDC-4E8A-AB57-A94FE2F2878C",
        "MCV_CAD_CLIENTE": "4B16816C-B017-418C-9378-C4B0A28B0ED3",
        "MCV_DNA": "C9DE68B4-E324-496C-B8A1-A7BC384720B3",
        "GOLDEN_ID_HASH": "7551FEAD-EB0E-4EB2-803E-2EF1345C306C",
    }
    # Dicionario de dados com o nome das data extensions e campos da tabela para gerar:  Schema, Nome da Tabela e Nome do csv
    data_extension_fields = {
        "MCV_LGPD_RAIA": [
            "golden_id",
            "bkl_lgpd_telefone",
            "bkl_lgpd_celular",
            "bkl_lgpd_push",
            "bkl_lgpd_email",
            "bkl_lgpd_pbm",
            "bkl_lgpd_des_ben_farm",
            "bkl_lgpd_com_mkt",
            "fl_lgpd_indefinido",
        ],
        "MCV_LGPD_DROGASIL": [
            "golden_id",
            "bkl_lgpd_telefone",
            "bkl_lgpd_celular",
            "bkl_lgpd_push",
            "bkl_lgpd_email",
        ],
        "MCV_CAD_CLIENTE": [
            "golden_id",
            "nome",
            "sobrenome",
            "email",
            "tel_celular",
            "tel_fixo_res",
            "cliente_raia",
            "cliente_drogasil",
            "optout_email",
            "bandeira_pref",
        ],
        "MCV_DNA": [
            "golden_id",
            "alergia_e_renite",
            "asma",
            "barba",
            "bebe",
            "cuidado_senior",
            "curativos",
            "diabetes_oral",
            "gestante",
            "higiene_basica",
            "higiene_feminina",
            "higiene_oral",
            "hipertensao",
            "ex_fiel_raia",
            "ex_fiel_drog",
            "churn_fiel_raia",
            "churn_fiel_drog",
            "multicanal",
            "digital",
            "dna_cronico",
            "perfil_beleza",
        ],
        "GOLDEN_ID_HASH": ["golden_id", "hash_id"],
    }
    # Dicionario de configuração para inserir arquivos CSV no GCS
    config = {
        "project": "raiadrogasil-280519",
        "bucketname": ["drogaraia_adsync", "drogasil_adsync"],
    }

    data_extension_keys = extract_keys(data_extension)

    # Busca as credenciais para autenticar na API
    credentials = get_credendials(cfg_initial["version"])
    # Autenticação na SFMC API
    auth = get_auth(credentials, cfg_initial["version"])

    # Consome a SFMC API atraves de seus endpoints de forma assincrona para buscar dados PII's
    start_consume_sfmc = time.time()
    asyncio.set_event_loop_policy(
        asyncio.WindowsSelectorEventLoopPolicy()
    )  # Police para Windows
    dados_piis = run(
        preparation(
            cfg_initial["round_max"],
            auth,
            data_extension["GOLDEN_ID_HASH"],
            data_extension_keys[4],
            data_extension_fields,
            config,
        )
    )

    end_consume_sfmc = time.time() - start_consume_sfmc
    print(f" SFMC Excution Time: {end_consume_sfmc}")
