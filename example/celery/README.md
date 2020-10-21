# Celery example

# run beat server
```bash
cd src
spaceone celery spaceone.work -m . -c ./spaceone/work/conf/custom_beat.yaml

```

# run worker server
```bash
cd src
 spaceone celery spaceone.work -m . -c ./spaceone/work/conf/work.yaml 
```

# add some schedule
```
cd src
python add_schdule
```