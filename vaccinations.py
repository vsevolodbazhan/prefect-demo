from datetime import datetime, timedelta
from io import StringIO

import pandas
import requests
from prefect import Flow, Parameter, task
from prefect.engine.results import LocalResult
from prefect.schedules import IntervalSchedule

VACCINATIONS_DATA_URL = 'https://github.com/owid/covid-19-data/raw/master/public/data/vaccinations/vaccinations.csv'

schedule = IntervalSchedule(
    start_date=datetime.utcnow() + timedelta(minutes=1),
    interval=timedelta(days=1),
)


@task
def extract_raw_vaccinations_data(vaccinations_data_url: str) -> pandas.DataFrame:
    response = requests.get(url=vaccinations_data_url)
    return pandas.read_csv(filepath_or_buffer=StringIO(response.text))


@task
def clean_vaccinations_data(raw_vaccinations_data: pandas.DataFrame) -> pandas.DataFrame:
    vaccinations_data = raw_vaccinations_data[['location', 'date', 'total_vaccinations']]
    vaccinations_data.dropna(inplace=True)
    return vaccinations_data.astype({'location': str, 'date': str, 'total_vaccinations': int})


@task
def transform_vaccination_data(clean_vaccinations_data: pandas.DataFrame) -> pandas.DataFrame:
    return clean_vaccinations_data.groupby(by=['location', 'date']).sum()


@task(result=LocalResult(dir='./vaccinations', location='{today}'))
def save_vaccinations_data(transformed_vaccination_data: pandas.DataFrame) -> str:
    return transformed_vaccination_data.to_csv()


with Flow('vaccinations', schedule=schedule) as flow:
    vaccinations_data_url = Parameter('vaccinations_data_url', default=VACCINATIONS_DATA_URL)

    raw_vaccinations_data = extract_raw_vaccinations_data(vaccinations_data_url=vaccinations_data_url)
    clean_vaccinations_data = clean_vaccinations_data(raw_vaccinations_data=raw_vaccinations_data)
    transformed_vaccination_data = transform_vaccination_data(clean_vaccinations_data=clean_vaccinations_data)
    save_vaccinations_data(transformed_vaccination_data=transformed_vaccination_data)

flow.register(project_name='covid')
