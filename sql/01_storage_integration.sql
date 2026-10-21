CREATE STORAGE INTEGRATION s3_taxi_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::568898409506:role/GlueS3AccessRole'
    STORAGE_ALLOWED_LOCATIONS = ('s3://nyc-taxi-001/processed/');
DESC INTEGRATION s3_taxi_integration; 