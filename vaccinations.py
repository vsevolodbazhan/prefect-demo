from io import StringIO

import pandas
import requests
from prefect import Flow, Parameter, task
from prefect.engine.results import LocalResult

VACCINATIONS_DATA_URL: str = (
    'https://github.com/owid/covid-19-data/raw/master/public/data/vaccinations/vaccinations.csv'
)


@task
def extract_raw_vaccinations_data(vaccinations_data_url: str) -> pandas.DataFrame:
    response = requests.get(url=vaccinations_data_url)
    return pandas.read_csv(filepath_or_buffer=StringIO(response.text))


@task
def clean_vaccination_data(raw_vaccinations_data: pandas.DataFrame) -> pandas.DataFrame:
    vaccination_data = raw_vaccinations_data[['location', 'date', 'total_vaccinations']]
    vaccination_data.dropna(inplace=True)
    return vaccination_data.astype({'location': str, 'date': str, 'total_vaccinations': int})


@task(result=LocalResult(dir='./vaccinations', location='{today}'))
def save_vaccinations_data(clean_vaccinations_data: pandas.DataFrame) -> str:
    return clean_vaccinations_data.to_csv(index=False)


with Flow('vaccinations') as flow:
    vaccinations_data_url = Parameter('vaccination_data_url', default=VACCINATIONS_DATA_URL)

    raw_vaccinations_data = extract_raw_vaccinations_data(vaccinations_data_url=vaccinations_data_url)
    clean_vaccination_data = clean_vaccination_data(raw_vaccinations_data=raw_vaccinations_data)
    save_vaccinations_data(clean_vaccinations_data=clean_vaccination_data)

flow.register(project_name='covid')
