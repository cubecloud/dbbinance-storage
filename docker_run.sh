#!/bin/bash
# Stitch *.env files into a single env-file (gitignored) and start the
# dbupdater container. .env files must exist on the build host:
#   BINANCEKEYS.env  (encrypted binance api KEY/SECRET + IV)
#   BINANCE_KEY.env  (BINANCE_KEY salt phrase)
#   PSGSQLKEYS.env   (encrypted DB USERNAME/PASSWORD + IV)
#   PSGSQL_KEY.env   (PSGSQL_KEY salt phrase)
cat *.env > .temp && sudo docker run \
    --cpu-shares 128 \
    --env-file=.temp \
    --net=host \
    -e PSGSQL_HOST_IP=192.168.1.145 \
    --name dbupdater \
    --restart always \
    dbbinance-image:latest && rm .temp
