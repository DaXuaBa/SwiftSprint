from datetime import datetime
from kafka import KafkaProducer
from dateutil import parser
import json
import pytz
import requests
from sqlalchemy.orm import Session
from db.models import *

def get_data_from_url():
    try:
        actors = "apidojo~tweet-scraper"
        token = "apify_api_RiD1m2rnJRRJBb8evfT5HkRIRyjVMB3RV3Tw"
        url = f"https://api.apify.com/v2/acts/{actors}/runs?token={token}"
        response = requests.get(url)
        
        item = response.json()
        return item
    except requests.exceptions.RequestException as e:
        print(f"An error occurred in get_data_from_url(): {e}")
        return None
    
def insert_data_to_database(db: Session):
    try:
        item = get_data_from_url()
        
        for x in item['data']['items']:
            defaultDatasetId = x['defaultDatasetId']

            exists = db.query(DatasetStatus).filter(DatasetStatus.dataset_id == defaultDatasetId).first()
            if not exists:
                new_status = DatasetStatus(dataset_id=defaultDatasetId)
                db.add(new_status)
                db.commit()
                db.refresh(new_status)
            else:
                print("defaultDatasetId already exists in table DatasetStatus")
    except Exception as e:
        print(f"An error occurred in insert_data_to_database: {e}")

def convert_to_vietnam_time(time_str):
    try:
        # Phân tích chuỗi thời gian thành đối tượng datetime
        time_utc = parser.parse(time_str)
        
        # Loại bỏ thông tin múi giờ để tạo đối tượng naive datetime
        time_utc = time_utc.replace(tzinfo=None)

        # Chuyển múi giờ từ UTC sang múi giờ Việt Nam
        utc_timezone = pytz.timezone('UTC')
        vietnam_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
        time_utc = utc_timezone.localize(time_utc)
        time_vietnam = time_utc.astimezone(vietnam_timezone)

        # Trả về đối tượng datetime dưới dạng chuỗi
        return time_vietnam.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        return None

def get_location_info(location):
    try:
        url = f"https://nominatim.openstreetmap.org/search?q={location}&format=json"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data:
                display_name = data[0]['display_name'].split(',')
                if len(display_name) >= 3:
                    state_1 = display_name[-3].strip()
                else:
                    state_1 = None
                
                if len(display_name) >= 2:
                    state_2 = display_name[-2].strip()
                else:
                    state_2 = None
                
                country = display_name[-1].strip()

                location_info = {
                    "latitude": data[0]['lat'],
                    "longitude": data[0]['lon'],  
                    "state_1": state_1,        
                    "state_2": state_2,
                    "country": country,
                }
                return location_info
            else:
                return None
        else:
            return None
    except Exception as e:
        return None
    
def send_data_to_kafka(data):
    try:
        # Khởi tạo producer Kafka
        producer = KafkaProducer(bootstrap_servers=['leesin.click:9092'],
                                 value_serializer=lambda x: json.dumps(x).encode('utf-8'))

        # Gửi dữ liệu vào Kafka topic
        producer.send('BidenTopic', value=data)

        # Đóng producer
        producer.close()
    except Exception as e:
        print(f"An error occurred while sending data to Kafka: {str(e)}")

def call_api(db: Session):
    print("Task in progress")
    insert_data_to_database(db)
    try:
        # Lấy các bản ghi từ cơ sở dữ liệu có status = 0
        records = db.query(DatasetStatus).filter(DatasetStatus.status == 0).all()

        # Lặp qua từng bản ghi
        for record in records:
            record_id = record.id
            dataset_id = record.dataset_id

            # Gọi API với default_dataset_id là dataset_id
            url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
            response = requests.get(url)

            # Kiểm tra xem request có thành công không
            if response.status_code == 200:
                # Đổi status của bản ghi từ 0 thành 1
                record.status = 1
                db.commit()

                for x in response.json():
                    tweet_id = x['id']
                    # Kiểm tra xem tweet_id đã tồn tại trong TweetData hay chưa
                    exists = db.query(TweetData).filter(TweetData.tweet_id == tweet_id).first()
                    if not exists:
                        created_at_str = x['createdAt']
                        tweet = x['text']
                        likes = x['likeCount']
                        retweet_count = x['retweetCount']
                        user_id = x['author']['id']
                        user_name = x['author']['userName']
                        user_screen_name = x['author']['name']
                        user_description = x['author']['description']
                        user_join_date_str = x['author']['createdAt']
                        user_followers_count = x['author']['followers']
                        user_location = x['author']['location']

                        location_info = get_location_info(user_location)
                        if location_info:
                            latitude = location_info.get('latitude', None)
                            longitude = location_info.get('longitude', None)
                            state_1 = location_info.get('state_1', None)
                            state_2 = location_info.get('state_2', None)
                            country = location_info.get('country', None)
                        else:
                            latitude, longitude, state_1, state_2, country = None, None, None, None, None
                        
                        # Chuyển đổi chuỗi ngày tháng thành đối tượng datetime
                        created_at = convert_to_vietnam_time(created_at_str)
                        user_join_date = convert_to_vietnam_time(user_join_date_str)

                        # Tạo đối tượng TweetData và thêm vào phiên làm việc
                        new_tweet = TweetData(
                            created_at=created_at, tweet_id=tweet_id, tweet=tweet, likes=likes, retweet_count=retweet_count,
                            user_id=user_id, user_name=user_name, user_screen_name=user_screen_name, user_description=user_description,
                            user_join_date=user_join_date, user_followers_count=user_followers_count, user_location=user_location,
                            latitude=latitude, longitude=longitude, state_1=state_1, state_2=state_2, country=country
                        )
                        db.add(new_tweet)
                        db.commit()
                        db.refresh(new_tweet)

                        states = {
                            'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA', 'Colorado': 'CO',
                            'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
                            'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA',
                            'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
                            'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
                            'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
                            'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD',
                            'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA',
                            'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'
                        }

                        if country == 'United States':
                            if state_1 in states:
                                state_to_send = state_1
                                state_code_to_send = states[state_1]
                            elif state_2 in states:
                                state_to_send = state_2
                                state_code_to_send = states[state_2]
                            else:
                                state_to_send = None  
                                state_code_to_send = None

                            if state_to_send:
                                send_data_to_kafka({
                                    "tweet": tweet,
                                    "state": state_to_send,
                                    "state_code": state_code_to_send
                                })

                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(f"Task {record_id} completed at {current_time}")
            else:
                print(f"Failed to call API for record {record_id}. Status code: {response.status_code}")
        print("Task has ended")
    except Exception as e:
        print(f"An error occurred in call_api(): {str(e)}")