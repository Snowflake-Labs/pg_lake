-- Add a secret for testbucketcdw
CREATE SECRET s3test (
    TYPE S3,
    KEY_ID 'testing',
    SECRET 'testing',
    ENDPOINT 'localhost:5999',
    SCOPE 's3://testbucketcdw',
    URL_STYLE 'path', USE_SSL false
);

-- Add a secret for testbucketgcs
CREATE SECRET gcstest (
    TYPE GCS,
    KEY_ID 'testing',
    SECRET 'testing',
    ENDPOINT 'localhost:5998',
    SCOPE 'gs://testbucketgcs',
    URL_STYLE 'path', USE_SSL false
);

-- Add a secret for testbucketr2
CREATE SECRET r2test (
    TYPE R2,
    KEY_ID 'testing',
    SECRET 'testing',
    ENDPOINT 'localhost:5997',
    SCOPE 'r2://testbucketr2',
    URL_STYLE 'path', USE_SSL false
);

-- Add a secret for testcontainer
CREATE SECRET aztest (
    TYPE AZURE,
    CONNECTION_STRING 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1' 
); 

-- Set a managed storage location
SET GLOBAL pg_lake_region TO 'ca-west-1';
SET GLOBAL pg_lake_managed_storage_bucket TO 's3://pglakemanaged1';
