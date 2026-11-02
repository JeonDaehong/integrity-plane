SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG gw WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://plane:8181',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://s3:8333',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'admin',
  's3.secret-access-key' = 'password',
  'client.region' = 'us-east-1'
);
