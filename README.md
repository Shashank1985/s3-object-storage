Heavily oversimplified implementation of AWS S3 in python as a learning project

***Features and Functionalities***

API endpoints for buckets:
1. create bucket (PUT /buckets/{bucket_name})
2. retrieve list of buckets (GET /buckets/buckets)
3. delete bucket by name (DELETE /buckets/{bucket_name})

API endpoints for objects:
1. upload objects to bucket (PUT /objects/{bucket_name}/{object_path})
2. retrieve object from bucket (GET /objects/{bucket_name}/{object_path})
3. delete object from bucket (DELETE /objects/{bucket_name}/{object_path})