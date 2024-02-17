# SimpleKeyValueDatabase
Implementation of a simple key value database, using lsm tree (memtable and ss table) for High Load Software Architecture course.
## Getting Started
These instructions will cover usage information of project on the docker container.
### Prerequisities
In order to run this container you'll need docker installed.
### Usage
#### Build Image
```shell
docker build -t SimpleKeyValueDatabaseImage .
```
#### Run Container
```shell
docker run -d -p 5555:8080 --name SimpleKeyValueDatabaseContainer SimpleKeyValueDatabaseImage
```
##### To specify memory table limit and ss table limit
```shell
docker run -d -p 5555:8080 -e MEM_TABLE_SIZE_LIMIT=30 -e SS_TABLE_COUNT_LIMIT=30 --name SimpleKeyValueDatabaseContainer SimpleKeyValueDatabaseImage
```
#### Environment Variables
* `MEM_TABLE_SIZE_LIMIT` - In memory table limit before flush. (Default = 5)
* `SS_TABLE_COUNT_LIMIT` - Sorted String Table count limit for compaction (Default = 5)
#### Volumes
* `/app/wwwroot/data` - File location of SS Tables dumps.
## Running
After running docker container, application will be avaiable at link below.
```link
http://localhost:5555/swagger/index.html
```
## Usage

You can make API calls to manage database.

Default interface will be Swagger but you can use any other Web API testing tool (e.g. Postman or Insomnia).

It allows you to operations below.
* Create (or Update) --> [POST] /api/keyvaluedatabase/
* Delete --> [DELETE] /api/keyvaluedatabase/{key}
* Read --> [GET] /api/keyvaluedatabase/{key}

Additionally you can download SS Tables as zip file. SS Table files will be json files and formatted as 
```link
yyyyMMddHHmmss-SSTable.json
```

Compacted version of SS Tables always will be named as 
```link
00000000000000-SSTable.json

```
## Screenshot
![App Screenshot](https://i.imgur.com/KAmIkir.png)
