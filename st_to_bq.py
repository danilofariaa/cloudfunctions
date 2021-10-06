import json
import requests
import time
import asyncio
import pandas as pd
import google
import aiohttp
from asyncio import gather, run

# from httpx import AsyncClient
from google.cloud import storage, bigquery
from datetime import datetime

from requests.exceptions import Timeout


def get_data_secret(version):
<<<<<<< HEAD
    if version == "v2":
=======
    if version == "v1":
        credentals = {
            "AUTH_URI_V1_RAIA": "https://mcjss9736km3nd134n6cv8-hcfy0.auth.marketingcloudapis.com/v1/requestToken",
            "REST_URI_V1_RAIA": "https://mcjss9736km3nd134n6cv8-hcfy0.rest.marketingcloudapis.com/",
            "CLIENT_ID_V1_RAIA": "",
            "CLIENT_SECRET_V1_RAIA": "",
        }
        return credentals
    elif version == "v2":
>>>>>>> 0ae244c41699fef00a0dc3a563389520cdb0879e
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
    # Args: Version(str) : Api version
    # 2 Passo : Coletar as informacoes de data secret, como endpoint para autenticacao,consumo bem como client e client secret
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
    # Args: Version(str) : Api version
    # 1 Passo : Pegar as credenciais do usuario para logar na API
    credentials = get_data_credentials(version)
    return credentials


def check_status_code(status):
    # Args: status : Status code of a request
    # check status code (can be called multiple times)

    if status >= 200 and status <= 299:
        return "success", status
    elif status >= 300 and status <= 399:
        return "redirect", status
    elif status >= 400 and status <= 499:
        return "error_client", status
    elif status >= 500 and status <= 599:
        return "error_server", status


def response_data(url, payload):
    # Args
    # Url : Url of request,
    # Payload: data of request

    # Make request from api versioned (can be called multiple times)
    # Return a dic with:
    # Access token, Token type, and Expiration time
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
    # Args: Version(str) : Api version
    # Get url auth from .env file
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
    # Args:
    # Payload: Authentication data
    # API Version: Version API
    # Make auth-request
    url = get_url_auth_dotenv(version)
    authentication = response_data(url, payload)
    return str(authentication)


async def download(session, url, token):
    """
    Task para realizar requisicao assincrona
    Args =  number : numero da iteracao
            token: token para autenticacao com api
            externalkey: chave para acesso a dataextension
    """
    headers = {"Authorization": token}
    async with session.get(url=url, headers=headers, timeout=None) as resp:
        data = await resp.json()
        return data


async def consume_sfmc(start, stop, token, externalkey):
    """
    A API possui uma restrição de 2500 registros por requisicao, dessa forma implementamos um loop para iterar sob o total de registros solicitado,
    dado pelo total de registros / 2500
    Args = start: range inicial para o consumo da api
           stop : range final para o consumo da api
           token : token de autenticacao sfmc
           externalkey : external key vinda da data extensions SFMC
    """
    print("Consumindo dados")
    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for number in range(start, stop):
            url = "https://mcjss9736km3nd134n6cv8-hcfy0.rest.marketingcloudapis.com/data/v1/customobjectdata/key/{externalkey}/rowset?$pageSize=2500&$page={number}"
            tasks.append(
                asyncio.ensure_future(
                    download(
                        session,
                        url.format(externalkey=externalkey, number=number),
                        token,
                    )
                )
            )
            await asyncio.sleep(0.001)

        pii_data = await asyncio.gather(*tasks)
        return pii_data


def unpack(list):
    """
    Desempacota os dados piis vindos da requisicao
    Args = lista de dados da requisicao  (vindas do request.content)
    """
    lst = []
    for item in list:
        for key, value in item.items():
            if key == "items":
                lst.append(value)
    return lst


def create_dataframe(data, de_key, de_fields):
    # Args
    # data : dict com os dados piis vindos da SFMC
    # Cria um dataframe e preenche dados NaN como NULL
    df = pd.DataFrame(data, columns=de_fields[de_key])
    df.fillna("NULL", inplace=True)

    return df


def csv_to_gcs(dataframe, data_extension: str, config):
    # Args
    # dataframe : dict with SFMC
    # brand : Raia or Drogasil
    # config : Project initial configs such as (project, bucketname)
    # Save file on GCS bucket (adsync)
    date = datetime.today().strftime("%Y-%m-%d")
    filepath = f"{data_extension}_pii_{date}.csv"
    uri = save_file_on_bucket(
        dataframe.to_csv(index=False),
        config["project"],
        config["bucketname"][0],
        filepath,
    )
    return uri


def save_file_on_bucket(
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


def create_schema(df):
    # Cria schema para uma tabela do BQ
    # Args = df: Dataframe pandas criado para os dados piis
    lst = df.columns.tolist()
    schema = []
    types = {"int": "INTEGER", "float": "FLOAT", "bool": "BOOLEAN", "str": "STRING"}
    for key in lst:
        t = str(type(key).__name__)
        if t in types:
            typ = types[t]
            schema.append(bigquery.SchemaField(key, typ))

    return schema


def gcs_to_bq(project, dataset_name, data_extension, schema, gcs_uri):
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
    table_name = f"{data_extension}_{datetime.today().strftime('%Y-%m-%d')}"
    print(f"Reporting  \n {dataset_name} => {table_name}  \n  {schema} \n {gcs_uri}")
    print("configurando job ... ")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = bq_client.load_table_from_uri(
        gcs_uri, dataset_ref.table(table_name), job_config=job_config
    )

    print("Executando job {}...".format(load_job.job_id))
    load_job.result()  # Waits for table load to complete.

    destination_table = bq_client.get_table(dataset_ref.table(table_name))
    print(f"[{table_name}] Sucesso no carregamento da tabela!")


def clean_pii(dados_pii):
    """
    Para cada requisicao, pega apenas os dados da chave items vindas da request
    Args =  dados_pii: lista de requests
    """
    lst = []
    count = 0
    for d in dados_pii:
        # body = json.loads(d.content)
        for key, value in d.items():
            if key == "items":
                count += 1
                lst.append(value)
    print(f"total de items {count}")
    return lst


def merge_pii(dp_clean):
    """
    Lista com dicionarios com todas as informacoes completas juntas
    Args : Dicionario com dados piis tratados

    """

    lst = []
    for i in dp_clean:
        for j in i:
            a = {**j["keys"], **j["values"]}
            lst.append(a)
    return lst


def extract_keys(data_extension):
    """
    Carrega as chaves da data extensions para formacao de nome de tabela
    Args = data_extension nome das chaves do dicionario de data extensions
    """
    lst = []
    de = data_extension.keys()
    for i in de:
        lst.append(i)
    return lst


if __name__ == "__main__":
    # API Version V2 uses OAuth2.0
    # Dicionario de dados com configurações iniciais : Versão API, Bandeira
    cfg_initial = {"version": "v2", "brand": ["raia", "drogasil"]}

    # Dicionario com as data extensions e external keys
    data_extension = {
        "MCV_LGPD_RAIA": "50594300-03D9-485D-9E4D-39CB331023B6",  # ok
        "MCV_LGPD_DROGASIL": "C1E4F6ED-DFDC-4E8A-AB57-A94FE2F2878C",  # ok
        "MCV_CAD_CLIENTE": "4B16816C-B017-418C-9378-C4B0A28B0ED3",
        "MCV_DNA": "C9DE68B4-E324-496C-B8A1-A7BC384720B3",
    }

    data_extension_fields = {
        "MCV_LGPD_RAIA": [
            "golden_id",
            "bkl_lgpd_telefone",
            "bkl_lgpd_celular",
            "bkl_lgpd_push",
            "bkl_lgpd_email",
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
    dados_piis = run(consume_sfmc(1, 401, auth, data_extension["MCV_DNA"]))

    end_consume_sfmc = time.time() - start_consume_sfmc
    print(f" SFMC Excution Time: {end_consume_sfmc}")

    # Desempacotamento/Tratamento dos dados vindos do consumo da SFMC API
    dp_clean = clean_pii(dados_piis)

    # Merging de dados vindos do processo de desempacotamento para transformar multiplos dicionarios com dados piis em um
    merged_data = merge_pii(dp_clean)

    # Transformação de dicionario de dados para dataframe
    df = create_dataframe(merged_data, data_extension_keys[3], data_extension_fields)
    print(df)

    # Dicionario de configuração para inserir arquivos CSV no GCS
    config = {
        "project": "raiadrogasil-280519",
        "bucketname": ["drogaraia_adsync", "drogasil_adsync"],
    }

    # Inserção do arquivo CSV (com dados piis) no GCS
    uri = csv_to_gcs(df, data_extension_keys[3], config)

    # Cria o schema das colunas do dataframe. Será passado como parametro na construção da tabela no BQ
    schema = create_schema(df)

    # Salva os dados Piis no BQ
    gcs_to_bq(config["project"], "data_adsync", data_extension_keys[3], schema, uri)
