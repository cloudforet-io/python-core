# Celery example

# run beat server
```bash
cd src
spaceone celery spaceone.work -m . -c ./spaceone/work/conf/work.yaml  beat

```

# run worker server
```bash
cd src
 spaceone celery spaceone.work -m . -c ./spaceone/work/conf/work.yaml  worker -c 8
```

# purge que
```
cd src
spaceone celery spaceone.work -m . -c ./spaceone/work/conf/work.yaml  purge -f
```