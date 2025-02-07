# Try-JMS

This project is about learning a bit about JMS. It uses a containerized JMS queue manager.
For that Docker image, see
[create and configure queue manager](https://developer.ibm.com/learningpaths/ibm-mq-badge/create-configure-queue-manager/).

## Preparation

Before running any of the programs such as `SimpleJmsProducer` and `SimpleJmsConsumer`, "install" MQ:

```shell
docker pull icr.io/ibm-messaging/mq:latest

docker volume create qm1data

docker run \
  --env LICENSE=accept \
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

In the [MQ coding challenge](https://developer.ibm.com/learningpaths/ibm-mq-badge/mq-coding-challenge/),
the event booking service Docker image could be built as follows:

```shell
docker build \
  --build-arg platformArch=amd64 \
  --build-arg baseImageRunStage="icr.io/ibm-messaging/mq" . -t mqbadge:latest
```

A container could be created and started from that image as follows:

```shell
docker run \
  -e LICENSE=accept \
  -e MQ_QMGR_NAME=QM1 \
  -e LOG_FORMAT=json \
  -e MQ_APP_USER=app \
  -e MQ_APP_PASSWORD=passw0rd \
  -e MQ_ADMIN_USER=admin \
  -e MQ_ADMIN_PASSWORD=passw0rd \
  -p 1414:1414 \
  -p 9443:9443 \
  --detach -ti --name mqebs \
  mqbadge:latest
```

## Reference material

This project uses reference and study material such as:
* [IBM MQ Developer Essentials](https://developer.ibm.com/learningpaths/ibm-mq-badge/)
