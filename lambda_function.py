import json
import os 
import re 
import time

import boto3
import requests
from datetime import datetime, timedelta, date
import xmltodict
from typing import List, Optional, Union, Any, Dict

from dotenv import load_dotenv

load_dotenv()

# 공공데이터 CONFIGURATION
WEATHER_OPENAPI_URL = os.getenv('WEATHER_OPENAPI_URL') if os.getenv('WEATHER_OPENAPI_URL') else os.environ.get('WEATHER_OPENAPI_URL')
WEATHER_OPENAPI_PARAMS = {
          'serviceKey': os.getenv('WEATHER_DATA_API_KEY') if os.getenv('WEATHER_DATA_API_KEY') else os.environ.get('WEATHER_DATA_API_KEY') ,
          'pageNo' : '1',
          'numOfRows': '1000',
          'dataType': 'XML',
          'base_date': datetime.today().strftime("%Y%m%d"),
          'base_time': '0200',
          'nx': 55,
          'ny': 127}

# AWS CONFIGURATION
AWS_CONFIG = {
    'access_key_id': os.getenv('ACCESS_KEY') if os.getenv('ACCESS_KEY') else os.environ.get('ACCESS_KEY'),
    'secret_access_key': os.getenv('SECRET_ACCESS_KEY') if os.getenv('SECRET_ACCESS_KEY') else os.environ.get('SECRET_ACCESS_KEY'),
    'region': os.getenv('REGION') if os.getenv('REGION') else os.environ.get('REGION'),
    'bucket_name': os.getenv('BUCKET') if os.getenv('BUCKET') else os.environ.get('BUCKET'),
    'prefix': os.getenv('PREFIX') if os.getenv('PREFIX') else os.environ.get('PREFIX'),
    'file_key': 'lambda_weather_data_test'
}



def get_weather_data(url: str, 
                     params: Dict[str, Union[str, int]],
                     max_retries: int = 10,
                     interval: int = 3) -> Dict[str, Any]:
    """
    OPEN API를 사용하여 날씨 정보를 가져오는 함수입니다.

    Args:
        url (str, optional): OPEN API URL. Defaults to OPENAPI_URL.
        params (Dict[str, str], optional): API 호출 파라미터. Defaults to dict().
        max_retries (int): 최대 재시도 횟수.
        interval (int): 실패 후 재시도 간격

    Returns:
        weather_data: 날씨 정보 딕셔너리
    """
    for _ in range(max_retries):
        try:
            res = requests.get(url=url, params=params, verify=False)
            res.raise_for_status()  # 이것이 HTTPError를 일으키면, except 블록으로 잡습니다.
            xml_data = res.text
            dict_data = xmltodict.parse(xml_data)
            # 요청 성공 시, 반복문 탈출
            return dict_data

        except requests.exceptions.HTTPError as errh:
            print(f"Http Error: {errh}")
        except requests.exceptions.ConnectionError as errc:
            print(f"Error Connecting: {errc}")
        except requests.exceptions.Timeout as errt:
            print(f"Timeout Error: {errt}")
        except requests.exceptions.RequestException as err:
            print(f"Something Else: {err}")
            
            # 지정된 시간만큼 대기 후 재시도
        time.sleep(interval)

    if dict_data:
        # dictionary initation
        weather_data = {}

        for item in dict_data['response']['body']['items']['item']:
            # 강수형태 : 없음(0), 비(1), 비/눈(2), 눈(3), 빗방울(5), 빗방울눈날림(6), 눈날림(7)
            if item['category'] == 'PTY':
                weather_data['pty'] = item['fcstValue']

            # 습도
            if item['category'] == 'REH':
                weather_data['reh'] = item['fcstValue']
            # 하늘상태: 맑음(1) 구름많은(3) 흐림(4)
            if item['category'] == 'SKY':
                weather_data['sky'] = item['fcstValue']
            # 일최저기온
            if item['category'] == 'TMN':
                weather_data['tmn'] = item['fcstValue']
            # 일최고기온
            if item['category'] == 'TMX':
                weather_data['tmx'] = item['fcstValue']
            # 풍향
            if item['category'] == 'VEC':
                weather_data['vec'] = item['fcstValue']
            # 풍속
            if item['category'] == 'WSD':
                weather_data['wsd'] = item['fcstValue']

        return weather_data

def process_weather_data(weather_data: str) -> bytes:
    '''
    weather_data 전처리
    '''
    encoded_data = bytes(json.dumps(weather_data).encode('UTF-8'))
    return encoded_data    

def upload_file_to_s3(bucket: str,
                   prefix: str, 
                   file_name: str, 
                   encoded_data: bytes) -> bool:
    """ 
    weather_data S3에 업로드. 
    파일이 이미 있을 경우 이름에 숫자를 증가시켜 파일 업로드. 
    """
    s3_client = boto3.client('s3',
                             aws_access_key_id=AWS_CONFIG['access_key_id'],
                             aws_secret_access_key=AWS_CONFIG['secret_access_key'],
                             region_name=AWS_CONFIG['region'])

    full_file_name = f"{prefix}{file_name}"
    new_file_name = get_existing_filename(s3=s3_client, 
                                          bucket=bucket, 
                                          file_name=full_file_name)

    # 실제 파일을 업로드
    try:
        s3_client.put_object(Bucket=bucket, Key=new_file_name, Body=encoded_data)
        return True
    except Exception as e:
        print(e)
        return False
    
def get_existing_filename(s3: object, bucket: str, file_name: str):
    '''
    해당 파일 이름이 존재하는지 안하는지 정규표현식으로 검출 후 incremental number로 대치
    '''
    # 파일명과 일치하거나 "_숫자"로 끝나는 파일들을 파악하기 위해 정규표현식 사용
    today_str = datetime.now().strftime("%Y%m%d")  # 오늘 날짜를 YYYYMMDD 형식의 문자열로 포맷
    file_name = f"{file_name}_{today_str}"
    pattern = re.compile(rf"^{re.escape(file_name)}(?:_(\d+))?$")

    # S3 버킷 내의 모든 객체 목록을 가져오고 파일명과 매칭되는지 확인
    all_objects = s3.list_objects_v2(Bucket=bucket, Prefix=file_name)
    existing_numbers = [int(match.group(1)) for obj in all_objects.get('Contents', [])
                        if (match := pattern.match(obj['Key'])) and match.group(1)]
    
    # "_숫자"에 맞는 파일들 중 가장 큰 숫자를 찾고, 그 다음 숫자를 반환
    max_number = max(existing_numbers, default=0)
    next_number = max_number + 1
    new_file_name = f"{file_name}_{next_number}"
    return new_file_name


def lambda_handler(event=None, context=None):
    weather_data = get_weather_data(url=WEATHER_OPENAPI_URL,
                                     params=WEATHER_OPENAPI_PARAMS)
    encoded_data = process_weather_data(weather_data=weather_data) 
    result = upload_file_to_s3(bucket=AWS_CONFIG['bucket_name'],
                               prefix=AWS_CONFIG['prefix'],
                               file_name=AWS_CONFIG['file_key'], 
                               encoded_data=encoded_data)

    if result:
        print('성공')
        return {
            'statusCode': 200,
            'body': json.dumps('Upload Success')
        }
    else:
        print('실패')
        return {
            'statusCode': 400,
            'body': json.dumps('Upload Fail')
        }

lambda_handler()