# Prefect Demo

Demo use-case of [Prefect](https://www.prefect.io).

## Purpose

Demonstrate a difference between an *orchestration* workflow management system (such as [Airflow](https://airflow.apache.org)) and a *data processing* workflow management system (which if what Prefect is).

Notable features for Airflow user:

1. A task is defined as a Python function decorated with `prefect.task` decorator.
2. Tasks accept incoming data as regular parameters. Those parameters don't go through the process of serialization and are not limited by a metastorage (unlike [Airflow XComs](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#xcoms)).
3. Dependencies between tasks are determined by looking at the input and output data of each task.

## What the use-case does

This partucular use-case loads latest [COVID-19 vaccination data](https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv), cleans it up and groups it by country and date.

## Usage

Make sure you have [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/install/) installed.

Create and active a virtual environment with a tool of your chose and install the requirments:

```bash
pip install -r requirements.txt
```

Configure the Prefect server:

```
prefect backend server
```

Start the server:

```
prefect server start
```

Run the agent (worker):

```
prefect agent local start
```

Navigate to `localhost:8080` and create a new project called `covid`.

Register `vaccinations` flow with:

```
python vaccinations.py
```

See the scheduled runs of the `vaccinations` flows and wait a minute for the first run to start.

## References

1. [Prefect Core Docs](https://docs.prefect.io/core/)
2. [Why not Airflow?](https://docs.prefect.io/core/getting_started/why-not-airflow.html)
3. [Positive and Negative Data Engineering](https://medium.com/the-prefect-blog/positive-and-negative-data-engineering-a02cb497583d)
