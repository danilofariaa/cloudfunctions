import json
import requests
import time
import asyncio
from asyncio import gather, run
from httpx import AsyncClient
import pandas as pd
from google.cloud import storage, bigquery
from datetime import datetime


def get_data_secret(version):
    if version == "v1":
        credentals = {
            "AUTH_URI_V1_RAIA": "https://mcjss9736km3nd134n6cv8-hcfy0.auth.marketingcloudapis.com/v1/requestToken",
            "REST_URI_V1_RAIA": "https://mcjss9736km3nd134n6cv8-hcfy0.rest.marketingcloudapis.com/",
            "CLIENT_ID_V1_RAIA": "",
            "CLIENT_SECRET_V1_RAIA": "",
        }
        return credentals
    elif version == "v2":
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
    # 2nd step get credentials from .env file
    c = get_data_secret(version)

    if version == "v1":
        credendials = {
            "clientId": c["CLIENT_ID_V1_RAIA"],
            "clientSecret": c["CLIENT_SECRET_V1_RAIA"],
        }
        return credendials
    elif version == "v2":
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
    # 1st step get credentials from dotenv and return credentials
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


async def download(number, token):
    url = "https://mcjss9736km3nd134n6cv8-hcfy0.rest.marketingcloudapis.com/data/v1/customobjectdata/key/4B16816C-B017-418C-9378-C4B0A28B0ED3/rowset?$pageSize=2500&$page={number}"
    headers = {"Authorization": token}
    async with AsyncClient() as client:
        response = await client.get(
            url.format(number=number), headers=headers, timeout=None
        )
        print(number)
        return response


async def consume_sfmc(start, stop, token):
    return await gather(*[download(number, token) for number in range(start, stop)])


def unpack(list):
    lst = []
    for item in list:
        for key, value in item.items():
            if key == "items":
                lst.append(value)
    return lst


def create_dataframe(data):
    # Args
    # data : dict with SFMC
    # Create pandas dataframe and fill NaN values
    df = pd.DataFrame(
        data,
        columns=[
            "golden_id",
            "nome",
            "sobrenome",
            "email",
            "tel_celular",
            "tel_fixo_res",
            "cliente_raia",
            "cliente_drogasil",
            "cliente_univers",
            "possui_app",
            "optout_email",
            "optout_sms",
            "optout_midias",
            "possui_app_raia",
            "possui_app_drogasil",
            "bandeira_pref",
        ],
    )
    df.fillna("NULL", inplace=True)

    return df


def get_data_list(em):
    dl = [json.loads(x.decode("utf-8")) for x in em]
    return dl


def csv_to_gcs(dataframe, brand: str, config):
    # Args
    # dataframe : dict with SFMC
    # brand : Raia or Drogasil
    # config : Project initial configs such as (project, bucketname)
    # Save file on GCS bucket (adsync)
    date = datetime.today().strftime("%Y-%m-%d")
    filepath = f"{brand}_pii_{date}.csv"
    uri = save_file_on_bucket(
        dataframe.to_csv(index=False), config["project"], config["bucketname"], filepath
    )
    return uri


def save_file_on_bucket(
    content: bytes,
    project: str,
    bucketname: str,
    filepath: str,
    content_type="text/plain",
):
    # Args
    # data : dict with SFMC
    # Create pandas dataframe and fill NaN values

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
    # Create schema for BQ Table
    lst = df.columns.tolist()
    schema = []
    types = {"int": "INTEGER", "float": "FLOAT", "bool": "BOOLEAN", "str": "STRING"}
    for key in lst:
        t = str(type(key).__name__)
        if t in types:
            typ = types[t]
            schema.append(bigquery.SchemaField(key, typ))

    return schema


def gcs_to_bq(project, dataset_name, brand, schema, gcs_uri):

    print("Inicializando Client .. ")
    bq_client = bigquery.Client(project)
    dataset_ref = bq_client.get_dataset(project + "." + dataset_name)
    table_name = f"{brand}_{datetime.today().strftime('%Y-%m-%d')}"
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
    lst = []
    for d in dados_pii:
        body = json.loads(d.content)
        for key, value in body.items():
            if key == "items":
                lst.append(value)
    return lst


if __name__ == "__main__":
    # API Version V2 uses OAuth2.0
    v = "v2"
    brand = "raia"
    # Get environments credencials for consume API
    p = get_credendials(v)
    # Authenticate with SFMC API
    t = get_auth(p, v)
    # Consume the API using the endpoints of SFMC docs
    start_consume_sfmc = time.time()
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    loop = asyncio.ProactorEventLoop()
    asyncio.set_event_loop(loop)
    dados_piis = run(consume_sfmc(1, 200, t))
    end_consume_sfmc = time.time() - start_consume_sfmc
    print(f" SFMC Excution Time: {end_consume_sfmc}")

    d = clean_pii(dados_piis)
    lst = []
    for i in d:
        for j in i:
            a = {**j["keys"], **j["values"]}
            lst.append(a)

    # Create dataframe
    df = create_dataframe(lst)
    print(df)
    # Set config for project
    config = {"project": "raiadrogasil-280519", "bucketname": "drogaraia_adsync"}
    # Uri for save csv file into BQ
    uri = csv_to_gcs(df, brand, config)
    # Collect Schema from packed data
    schema = create_schema(df)
    # Save GCS csv file into BQ
    gcs_to_bq(config["project"], "data_adsync", brand, schema, uri)
