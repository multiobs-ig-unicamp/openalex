import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

import os
import glob
import pandas as pd
import json

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

job_oa = {
        'bucket_name': 'openalex',  
        'prefix': "data/works/",
        'entity': "works",
        'entity_singular': "work",
        'project_id': 'insyspo',
        'dataset': 'projectdb_openalex_2025_12',
        'dataset_id': 'insyspo.projectdb_openalex_2025_12',
        'total_chunks': 100
}

job_info = './job_info/'

def update_job_status(job_oa):

    filename = job_info+'job_'+job_oa['entity']+'.json'

    with open(filename, 'w') as json_file:
        json.dump(job_oa, json_file, indent=2)

    return job_oa

def read_job_status(entity):

    filename = job_info+'job_'+entity+'.json'

    with open(filename, 'r') as json_file:
        job_oa = json.load(json_file)

    return job_oa

def list_objects(job_oa):

    bucket_name = job_oa['bucket_name']
    prefix = job_oa['prefix']
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    response = s3_client.list_objects_v2(Bucket=bucket_name, 
                                         Prefix=prefix,
                                         MaxKeys=1000000)
    pieces = [x['Key'] for x in response['Contents'] if 'part_' in x['Key']]
    if 'NextContinuationToken' in response:
        next = response['NextContinuationToken']

        while 'NextContinuationToken' in response:
            response = s3_client.list_objects_v2(Bucket=bucket_name, 
                                                Prefix=prefix,
                                                MaxKeys=1000000,
                                                ContinuationToken=next)
            pieces += [x['Key'] for x in response['Contents']]
            if 'NextContinuationToken' in response:
                next = response['NextContinuationToken']

    T = pd.DataFrame(pieces, columns=['objects'])
    T.to_csv(job_info+job_oa['workload_file'], index=False)

    return pieces

def select_chunk(job_oa):

    chunk_file = job_oa['files_job'][0]

    T = pd.read_csv(chunk_file)

    objects = list(T['objects'])

    job_oa['files_job'] = job_oa['files_job'].remove(chunk_file)
    job_oa['files_job_running'] = job_oa['files_job_running'].append(chunk_file)

    update_job_status(job_oa)
    print(f"{len(objects)} selected to process from {chunk_file}")

    return objects, chunk_file


def create_dataset(dataset_id):

    bq_client = bigquery.Client()

    try:
        bq_client.get_dataset(dataset_id)  # Make an API request.
        print("Dataset {} already exists".format(dataset_id))
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)

        dataset.location = "US"

        dataset = bq_client.create_dataset(dataset, timeout=30)  
        print("Created dataset {}.{}".format(bq_client.project, dataset.dataset_id))

def upload_bigquery(file_path, table_id, entity_singular):

    bq_client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        schema= [{'name': entity_singular, 'type': 'STRING'}],
        field_delimiter="\t",
        write_disposition="WRITE_APPEND",
    )

    with open(file_path, "rb") as source_file:
        job = bq_client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result() 

    table = bq_client.get_table(table_id)
    print(
        f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}"
    )

def download_object(bucket_name,piece):

    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    folder = '\\'.join(piece.split("/")[:-1])
    filename = piece.split("/")[-1].split(".gz")[0]+".gz"
    os.makedirs('/'.join(piece.split("/")[:-1]), exist_ok=True)
    filepath = folder+'\\'+filename
    s3_client.download_file(bucket_name, piece, filepath) 

    return filepath


def download_upload(job_oa):

    dataset = job_oa['dataset']
    entity = job_oa['entity']  
    entity_singular = job_oa['entity_singular']
    bucket_name = job_oa['bucket_name']
    pieces, chunk_file = select_chunk(job_oa)
    if len(pieces) > 1:
        for p in pieces:
            fp = download_object(bucket_name, p)

            upload_bigquery(fp, dataset+'.'+entity, entity_singular)

            os.remove(fp)

            print(f"Processed object: {p}")

        job_oa['files_job_running'].remove(chunk_file)
        if len(job_oa['files_job_running']) == 0:
            job_oa['files_job_running'] = []
        job_oa = update_job_status(job_oa)
    else:
        print("No more pieces to process.")


def configure_job(job_oa):

    workload_file = job_oa['entity']+'_objects.csv'
    job_oa['workload_file'] = workload_file
    filename = job_info+'job_'+job_oa['entity']+'.json'

    if os.path.exists(filename):
        print(f"Existing job for {job_oa['entity']}. Continuing...")

        job_oa = read_job_status(job_oa['entity'])
        return job_oa
    else:
        print(f"Create job file in: {filename} for entity: {job_oa['entity']}.")

        list_objects(job_oa)
        create_dataset(job_oa['dataset_id'])
        os.makedirs(job_oa['prefix'], exist_ok=True)

        split_job(job_oa, total_chunks=job_oa['total_chunks'])

    return job_oa

def split_job(job_oa, total_chunks):

    T = pd.read_csv(job_info+job_oa['workload_file'])
    total_objects = T.shape[0]
    chunk_size = total_objects // total_chunks + 1
    files_job = []
    for i in range(total_chunks):
        chunk = T.loc[i*chunk_size:((i+1)*chunk_size-1),'objects']
        file_job_name = job_info+f"chunk_{i}_{job_oa['entity']}.csv"
        files_job.append(file_job_name)
        chunk.to_csv(file_job_name, index=False)

    job_oa['files_job'] = files_job
    job_oa['files_job_running'] = []
    update_job_status(job_oa)

    return job_oa



def prepare_job(job_oa):
    job_oa = configure_job(job_oa)

    return job_oa

if __name__ == "__main__":


    job_oa = prepare_job(job_oa)
    download_upload(job_oa)

    print(job_oa)

 