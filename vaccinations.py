from io import StringIO

import pandas
import requests
from prefect import Flow, Parameter, task
from prefect.engine.results import LocalResult

VACCINATIONS_DATA_URL: str = (
    'https://github.com/owid/covid-19-data/raw/master/public/data/vaccinations/vaccinations.csv'
)


@task
def extract_vaccinations_data(vaccinations_data_url: str) -> pandas.DataFrame:
    response = requests.get(url=vaccinations_data_url)
    return pandas.read_csv(filepath_or_buffer=StringIO(response.text))


@task(result=LocalResult(dir='./vaccinations', location='{today}'))
def save_vaccinations_data(vaccinations_data: pandas.DataFrame) -> str:
    return vaccinations_data.to_csv(index=False)


with Flow('vaccinations') as flow:
    vaccinations_data_url: str = Parameter('vaccination_data_url', default=VACCINATIONS_DATA_URL)

    vaccinations_data = extract_vaccinations_data(vaccinations_data_url=vaccinations_data_url)

    save_vaccinations_data(vaccinations_data=vaccinations_data)

flow.register(project_name='covid')
