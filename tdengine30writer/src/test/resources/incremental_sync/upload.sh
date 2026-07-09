#!/bin/bash

scp t2dm-restful.json root@192.168.56.105:/root/workspace/tmp/datax/job
scp t2dm-jni.json root@192.168.56.105:/root/workspace/tmp/datax/job
scp dm2t-restful.json root@192.168.56.105:/root/workspace/tmp/datax/job
scp dm2t-jni.json root@192.168.56.105:/root/workspace/tmp/datax/job
scp dm2t-update.json root@192.168.56.105:/root/workspace/tmp/datax/job
scp csv2t-restful.json root@192.168.56.105:/root/workspace/tmp/datax/job
scp csv2t-jni.json root@192.168.56.105:/root/workspace/tmp/datax/job


scp dm2t_sync.sh root@192.168.56.105:/root/workspace/tmp/datax
scp clean_env.sh root@192.168.56.105:/root/workspace/tmp/datax