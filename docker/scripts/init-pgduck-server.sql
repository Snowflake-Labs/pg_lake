-- create localstack aws secret
CREATE SECRET s3_localstack_secret(
        TYPE s3, 
        scope 's3://testbucket', 
        use_ssl false, 
        key_id 'test', 
        secret 'test', 
        url_style 'path', 
        endpoint 'localstack:4566',
        region 'us-east-1'
);
