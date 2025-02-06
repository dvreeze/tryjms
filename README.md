# Try-JMS

This project is about learning a bit about JMS. It uses a containerized JMS queue manager.
For that Docker image, see
[create and configure queue manager](https://developer.ibm.com/learningpaths/ibm-mq-badge/create-configure-queue-manager/).

## Preparation

Before running any of the programs, "install" MQ:

```shell
docker pull icr.io/ibm-messaging/mq:latest

docker volume create qm1data

docker run --env LICENSE=accept \
  --env MQ_QMGR_NAME=QM1 \
  --volume qm1data:/mnt/mqm \
  --publish 1414:1414 \
  --publish 9443:9443 \
  --detach \
  --env MQ_APP_USER=app \
  --env MQ_APP_PASSWORD=passw0rd \
  --env MQ_ADMIN_USER=admin \
  --env MQ_ADMIN_PASSWORD=passw0rd \
  --name QM1 \
  icr.io/ibm-messaging/mq:latest
```

## Reference material

This project uses reference and study material such as:
* [IBM MQ Developer Essentials](https://developer.ibm.com/learningpaths/ibm-mq-badge/)
