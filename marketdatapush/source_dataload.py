from google.cloud import storage
import os
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta

# Define a task
@task(log_prints=True, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def init_env_params(dataset_name: str):
    # set the credentials environment variable
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/dezoomcamp-project/configuration/dezoomcamp-4523-7f2f120d37fa.json'
    # Set the target bucket and folder path in the Google Cloud Storage
    bucket_name = 'market-datastore'
    folder_path = '{dataset_name}/'
    # set the path to your local directory containing .txt files
    local_dir = '/workspaces/dezoomcamp-project/data/{dataset_name}'
    return bucket_name, folder_path, local_dir

@flow(name="Ingest Flow")
def upload_files_to_gcs(bucket_name: str, folder_path: str, local_dir: str):  
    # create a storage client
    client = storage.Client()
    # Create a list of files in the local directory
    file_list = [f for f in os.listdir(local_dir) if f.endswith('.txt')]

    # Upload each file to the specified folder path in the Google Cloud Storage bucket
    for file_name in file_list:
        # Create a blob object with the same name as the local file
        blob = client.bucket(bucket_name).blob(folder_path + file_name)
        if blob.exists():
            # print(f'{file_name} already exists in GCS. Skipping...')
            skip_counter += 1
        else:
        # Upload the local file to the Google Cloud Storage bucket
            with open(os.path.join(local_dir, file_name), 'rb') as f:
                blob.upload_from_file(f)
                upload_counter += 1
    
    print(f'{skip_counter} files skipped to {folder_path} in {bucket_name} bucket')          
    print(f'{upload_counter} files uploaded to {folder_path} in {bucket_name} bucket')
    
def main_flow():
    #task: init_env_params
    bucket_name, folder_path, local_dir = init_env_params("stocks")
    upload_files_to_gcs(bucket_name, folder_path, local_dir)
   
if __name__ == '__main__':
    main_flow()
            