import json
import requests
import pandas as pd
from google.cloud import storage, bigquery
from datetime import datetime


def get_data_secret(version):
    if version == 'v1':
        credentals = {
            'AUTH_URI_V1_RAIA': 'https://mcjss9736km3nd134n6cv8-hcfy0.auth.marketingcloudapis.com/v1/requestToken',
            'REST_URI_V1_RAIA': 'https://mcjss9736km3nd134n6cv8-hcfy0.rest.marketingcloudapis.com/',
            'CLIENT_ID_V1_RAIA': 'o09bav0efhzkub1fjtngg2kf',
            'CLIENT_SECRET_V1_RAIA': 'QiWxOZR0fPB3yeJUbBQMxE6G'
        }
        return credentals
    elif version == 'v2':
        credentals = {
            'AUTH_URI_V2_RAIA': 'https://mcjss9736km3nd134n6cv8-hcfy0.auth.marketingcloudapis.com/v2/token',
            'REST_URI_V2_RAIA': 'https://mcjss9736km3nd134n6cv8-hcfy0.rest.marketingcloudapis.com/',
            'CLIENT_ID_V2_RAIA': 'o09bav0efhzkub1fjtngg2kf',
            'CLIENT_SECRET_V2_RAIA': 'QiWxOZR0fPB3yeJUbBQMxE6G'
        }
        return credentals
    else:
        return 'version not supported'


def get_data_credentials(version):
    # Args: Version(str) : Api version
    # 2nd step get credentials from .env file
    c = get_data_secret(version)

    if version == 'v1':
        credendials = {
            "clientId": c['CLIENT_ID_V1_RAIA'],
            "clientSecret": c['CLIENT_SECRET_V1_RAIA']
        }
        return credendials
    elif version == 'v2':
        credendials = {
            'client_id': c['CLIENT_ID_V2_RAIA'],
            'client_secret': c['CLIENT_SECRET_V2_RAIA'],
            'grant_type': 'client_credentials'
        }
        return credendials
    else:
        return 'version not supported'


def get_credendials(version):
    # Args: Version(str) : Api version
    # 1st step get credentials from dotenv and return credentials
    credentials = get_data_credentials(version)
    return credentials


def check_status_code(status):
    # Args: status : Status code of a request
    # check status code (can be called multiple times)

    if status >= 200 and status <= 299:
        return 'success', status
    elif status >= 300 and status <= 399:
        return 'redirect', status
    elif status >= 400 and status <= 499:
        return 'error_client', status
    elif status >= 500 and status <= 599:
        return 'error_server', status


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
    if check_status == 'success':
        auth_response = {
            "token": body['access_token'],
            "token_type": body['token_type'],
            "expiration": body['expires_in']
        }
        return str(auth_response['token_type'])+' '+str(auth_response['token'])
    else:
        return status


def get_url_auth_dotenv(version):
    # Args: Version(str) : Api version
    # 4th step get url auth from .env file
    auth = get_data_secret(version)
    if version == 'v1':
        url = auth['AUTH_URI_V1_RAIA']
        return url
    elif version == 'v2':
        url = auth['AUTH_URI_V2_RAIA']
        return url
    else:
        return 'version not supported'

def get_auth(payload, version):
    # 3rd step
    # Make auth-request
    url = get_url_auth_dotenv(version)
    authentication = response_data(url, payload)
    return str(authentication)


def consume_sfmc(token, version):
    rest = get_data_secret(version)
    url = rest['REST_URI_V2_RAIA']
    url += '/data/v1/customobjectdata/key/IGO_PROFILES/rowset?$pageSize=10'

    headers = {'Authorization': token}
    response = requests.get(url=url, headers=headers)
    check_status, status = check_status_code(response.status_code)

    if check_status == 'success':
        body = json.loads(response.content)
        return body
    else:
        return status


def unpack_data(dict):
    d = {}
    for key, value in dict.items():
        for k, v in value.items():
            if bool(v):
                d[k] = v
            else:
                value.pop('k', None)
    return d


def create_dataframe(data):
    df = pd.DataFrame(
        data, columns=['user_id', 'email', 'city', 'region', 'country'])
    df.fillna("NULL", inplace=True)
    return df


def csv_to_gcs(dataframe, brand: str, config):
    date = datetime.today().strftime('%Y-%m-%d')
    filepath = f"{brand}_pii_{date}.csv"
    uri = save_file_on_bucket(
        dataframe.to_csv(index=False), config['project'], config['bucketname'], filepath)
    return uri


def save_file_on_bucket(
    content: bytes,
    project: str,
    bucketname: str,
    filepath: str,
    content_type="text/plain"
):
   
    storage_client = storage.Client(project)
    uri = f"gs://{bucketname}/{filepath}"

    print(f"{filepath} Saving in Bucket...")

    try:
        bucket = storage_client.bucket(bucketname)
        blob = bucket.blob(filepath)
    except google.cloud.exceptions.NotFound:
        print(
            f"[ERROR] Storage Failure. "
            f"Bucket gs://{bucketname} not Found."
        )
        return

    blob.upload_from_string(content, content_type=content_type)
    print(f"{uri} file loaded successfully!")
    return uri


def create_schema(column_name):
    schema = []
    types = {
        'int': "INTEGER",
        'float': "FLOAT",
        'bool': "BOOLEAN",
        'str': "STRING"
    }
    for key, value in column_name.items():
        t = str(type(key).__name__)
        if t in types:
            typ = types[t]
            schema.append(bigquery.SchemaField(value, typ))
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
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="extraction_date",
        ),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = bq_client.load_table_from_uri(
        gcs_uri, dataset_ref.table(table_name), job_config=job_config
    )

    print("Executando job {}...".format(load_job.job_id))
    load_job.result()  # Waits for table load to complete.

    destination_table = bq_client.get_table(dataset_ref.table(table_name))
    print(f"[{table_name}] Sucesso no carregamento da tabela!")



    
    return 'aa'


if __name__ == '__main__':
    # API Version V2 uses OAuth2.0
    v = 'v2'
    brand = 'raia'
    # Get environments credencials for consume API
    p = get_credendials(v)
    # Authenticate with SFMC API
    t = get_auth(p, v)
    # Consume the API using the endpoints of SFMC docs
    em = consume_sfmc(t, v)
    # Gather data retreived from API
    user_data = em['items']
    # Data Wrangling  = Packing, Unpacking data for transfromation
    lst = []
    for u in user_data:
        unpacked = unpack_data(u)
        lst.append(unpacked)

    # Create dataframe
    df = create_dataframe(lst)

    # gcs to bq
    config = {
        'project': "raiadrogasil-280519",
        'bucketname': "drogaraia_adsync"
    }
    uri = csv_to_gcs(df, brand, config)
    schema = create_schema(lst[0])
    gcs_to_bq(config["project"], "data_adsync", brand, schema, uri)
