<!-- 

First create the spark master and worker containers using their respective docker files

to check the communication/ services in same network

Docker network ls 
Docker network inspect 'network id' (from above cmd)

if you are not able to see the service,  you need to get the container id and connect using the below cmd
docker network connect 'network id' 'container_id'


sync networks
docker network connect astro-dev_57fbec_airflow minio
-->