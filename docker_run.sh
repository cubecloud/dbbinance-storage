cat *.env > temp && sudo docker run --cpu-shares 128 --env-file=temp --net=host -e PSGSQL_HOST_IP=192.168.1.145 --name dbupdater --restart always dbbinance-image && rm temp
