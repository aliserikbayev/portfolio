from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json
import requests
import pandas as pd 
from airflow.sensors.base_sensor_operator import BaseSensorOperator



def kelvin_to_celsius(temp_in_kelvin): 
    return (temp_in_kelvin - 273.15)


def transform_load_coordinates(task_instance):
    data = task_instance.xcom_pull(task_ids='extract_coordinates')
    task_instance.xcom_push(key='my_variables', value={'latitude': data[0]['lat'], 'longitude': data[0]["lon"]})

def pull_data(**kwargs):
    data_from_xcom = kwargs['ti'].xcom_pull(task_ids='transform_coordinates', key='my_variables')
    return data_from_xcom

def transform_load_weather(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    transformed_data = {
        "weather": data['weather']['main'],
        'description': data['weather']['description'],
        "temperature": kelvin_to_celsius(data['main']['temp']),
        "feels like": kelvin_to_celsius(data['main']['feels_like']),
        "humidity": data['main']['humidity'],
        "pressure": data['main']['pressure'],
        'visibility': data['visibility'],
        'wind speed': data['wind']['speed'],
        'wind degree': data['wind']['deg'],
        'wind gust': data['wind']['gust'],
        "time(local time )": datetime.utcfromtimestamp(data['dt'] + data['timezone']),
        "sunrise(local time)": datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone']),
        "sunset(local time)": datetime.utcfromtimestamp(data['sys']["sunset"] + data['timezone'])
    }

    aws_credentials = {"key": "ASIA4MTWMNOLLNWQRQ4B" , "secret": "pp3TBpofwRnv0vOVsSl6T7/ej+FE6cCUIoAxKI80", "token": "IQoJb3JpZ2luX2VjEHcaCmV1LW5vcnRoLTEiRzBFAiEAoSAzV6qAjH5gTYpd+T2UHj0Eblhql77YCSbNdtYevmECIAxPqZP0iH2NU8ylokLb3gOzDTH+LA6a51iuVppT1DkHKtwBCKD//////////wEQABoMODUxNzI1NTQ0MzQyIgzCj05Kp0OD1QRya7UqsAFboqMlEOAUQCD2zwqw4NBKHAISaVLG3kndVk1OnFqex/ZcEoewUfy8xDnKzwNwmhnffpBS7onNAtG1i+2n1qsDktpd1sZKa8A6caFByyVn1EUVu4M4StMSqdcjzNNylKlG0kWFOxEvpJV6OwCPtIptu7qOwc1tJrgjGBcSbYJC+oAVHpn9chUhbNQXNdFCQryi8whnO+2q4HJClJG2qo58MP400CSg7OAQvB8RqHeyPzDEyr6wBjqYAYXV6URGosurFDKfI3livkmTu1b57T45ZJd3k0Rla/mfOXCrcHhrbinvNiH2kFr/cDSU4nT+/XB7bWvXXMk9oPJWtvQNxEf6dpev3BYDzb2EogHeOCt/7RYjYhW0jKyi7DA3mCtUWZb7trIBG0SPvk26b/QzOjn/D2Qd06n2K39V9ev8u0FFTeJ/w2hDVIrO4fqokIykGdiz"}
    transformed_data_list = [transformed_data]
    df_data =pd.DataFrame(transformed_data_list)

    now = datetime.now()
    dt_string = now.strftime('%d%m%y')
    dt_string = 'current_weather_data_ust-kamenogorsk' + dt_string
    df_data.to_csv(f"s3://weather-data-bucket-yml/{dt_string}.csv", index = False, storage_options=aws_credentials)


class CustomHttpSensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def poke(self, context):
        data = context['task_instance'].xcom_pull(task_ids='transform_coordinates', key="my_variables" )
        latitude = data['latitude']
        longitude = data['longitude']

        response = requests.get(f"/data/2.5/weather?lat={data['latitude']}lon={data['longitude']}&appid=606bfd68a2e76d8503d3e2fd051f1dc7")

        return response.status_code == 200


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'email': ['koltykwawali@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG('weather-dag',default_args=default_args,schedule_interval='@daily',catchup=False) as dag:
    is_coordinates_api_ready = HttpSensor(
        task_id = 'is_coordinates_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/geo/1.0/direct?q=Ust-Kamenogorsk,KZ&limit=2&appid=606bfd68a2e76d8503d3e2fd051f1dc7'
   )

    extract_coordinates = SimpleHttpOperator(
        task_id = "extract_coordinates",
        http_conn_id = 'weathermap_api',
        endpoint = '/geo/1.0/direct?q=Ust-Kamenogorsk,KZ&limit=2&appid=606bfd68a2e76d8503d3e2fd051f1dc7',
        method = 'GET',
        response_filter = lambda r: json.loads(r.text),
        log_response = True
    )

    transform_load_coordinates_data = PythonOperator(
        task_id = "transform_coordinates",
        python_callable = transform_load_coordinates
    )

    pull_coordinates_data = PythonOperator(
        task_id = "pull_coordinates",
        python_callable = pull_data
    )

    is_weather_api_ready = CustomHttpSensor(
        task_id = 'is_weather_api_ready',
        http_conn_id='weathermap_api',
   )

    extract_weather = SimpleHttpOperator(
        task_id = 'extract_weather',
        http_conn_id = 'weathermap_api',
        method = 'GET',
        endpoint = f"/data/2.5/weather?lat='{data_from_xcom['latitude']}lon={data_from_xcom['longitude']}&appid=606bfd68a2e76d8503d3e2fd051f1dc7",
        response_filter = lambda r: json.loads(r.text),
        log_response = True
    )

    transform_load_weather_data = PythonOperator(
        task_id = "transform_weather",
        python_callable = transform_load_weather
    )

    is_coordinates_api_ready >> extract_coordinates >> pull_coordinates_data >> is_weather_api_ready >> extract_weather >> transform_load_weather_data
    


    
