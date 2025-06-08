Heavily oversimplified implementation of AWS S3 in python as a learning project

**Features and Functionalities**

API endpoints for buckets:
1. create bucket (PUT /buckets/{bucket_name})
2. retrieve list of buckets (GET /buckets/buckets)
3. delete bucket by name (DELETE /buckets/{bucket_name})

API endpoints for objects:
1. upload objects to bucket (PUT /objects/{bucket_name}/{object_path})
2. retrieve object from bucket (GET /objects/{bucket_name}/{object_path})
3. delete object from bucket (DELETE /objects/{bucket_name}/{object_path})

Caching 

Implemented a simple LRUCache which stores the metadata of objects. I have used a Write Invalidation strategy. The cache keys is a tuple (bucket_name,object_name)

The workflow is:
1. Client makes a PUT request to upload object to the bucket
2. The metadata for that object is either inserted(if new object) or updated(if object is overwritten) in the metadata database
3. If the object metadata exists within the cache, it is deleted to prevent inconsistencies.
4. When the client makes a GET request for that object, it checks the cache (the get request made immediately after uploading will result in a cache miss) first.
5. Whenever the db is queried for the metadata, the metadata is stored in the cache and is accessed through cache later on.

**How to run**
1. Clone the repo
2. pip install -r requirements.txt
3. use curl or postman to test out the api endpoints
