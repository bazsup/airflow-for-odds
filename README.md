# airflow for odds

## start ENV
```bash
source ENV/bin/activate
```

## setup airflow home
```bash
export AIRFLOW_HOME=/path/to/airflow
```

## initial database
```bash
airflow initdb
```

## start web server
```bash
airflow webserver
```

## start airflow scheduler
```bash
airflow scheduler
```

## run specific airflow task
```bash
airflow test <DAG_NAME> <TASK_ID> <datetime>
```

