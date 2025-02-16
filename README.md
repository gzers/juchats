部署方法：

1. 下载后在根目录执行

```shell

docker build -t juchats-docker

```

2. docker-cli

```shell

docker run \
  -d \
  --name='Juchats-API' \
  --privileged=true \
  -e TZ="Asia/Shanghai" \
  -e 'PYTHONUNBUFFERED'='1' \
  -p '18002:8000/tcp' \
  -v [路径]:'/app':'rw' \
  'juchats-docker:latest'

```

3. 将源码放到/app映射路径下

4. 在网页控制台找到 `https://www.juchats.com/gw/chatweb/gpt/modes` 接口，找到jtoken，复制其值

5. api配置

    5.1. api：http://[ip]:18002/v1/new_normal

    5.3. api密钥：jtoken的值

    5.4. 模型：deepseek-ai/deepseek-r1

        


