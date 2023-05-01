from google.cloud import storage
import os

# set the credentials environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/dezoomcamp-project/configuration/dezoomcamp-4523-7f2f120d37fa.json'

# create a storage client
client = storage.Client()

# get the bucket where you want to upload the files
bucket = client.get_bucket('market-datastore')

# Set the target bucket and folder path in the Google Cloud Storage
bucket_name = 'market-datastore'
folder_path = 'stocks/'

# set the path to your local directory containing .txt files
local_dir = '/workspaces/dezoomcamp-project/data/stocks'

file_list = [f for f in os.listdir(local_dir) if f.endswith('.txt')]

skip_counter = 0
upload_counter = 0
# Upload each file to the specified folder path in the Google Cloud Storage bucket
for file_name in file_list:
    # Create a blob object with the same name as the local file
    blob = client.bucket(bucket_name).blob(folder_path + file_name)
    if blob.exists():
        print(f'{file_name} already exists in GCS. Skipping...')
        skip_counter += 1
    else:
    # Upload the local file to the Google Cloud Storage bucket
        with open(os.path.join(local_dir, file_name), 'rb') as f:
            blob.upload_from_file(f)
            upload_counter += 1
    
print(f'{skip_counter} files skipped to {folder_path} in {bucket_name} bucket')          
print(f'{upload_counter} files uploaded to {folder_path} in {bucket_name} bucket')  
            