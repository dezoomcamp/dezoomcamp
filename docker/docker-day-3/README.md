## How to run

```
# build docker image
docker build -t <image-given-name>:<image-given-tag> .

# docker container from the image
docker run -it --network host ingestion_pipeline:v002  --user=<postgres-username> --password=<postgres-password> --host=localhost --port=5432 --db=<postgres-db> --table=<postgres-table> --url=<path/to/csvFile>


# create a container for running postgres
docker run -it -e POSTGRES_USER="<postgres-username>" -e POSTGRES_PASSWORD="<postgres-password>" -e POSTGRES_DB="<postgres-db>" -v <path/to/volume>:/var/lib/postgresql/data -p 5432:5432 postgres:13


```
