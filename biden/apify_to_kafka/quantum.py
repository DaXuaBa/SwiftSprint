from datetime import datetime
from kafka import KafkaProducer
from dateutil import parser
import json
import pytz
import requests
import pymysql

# Kết nối đến cơ sở dữ liệu MySQL
connection = pymysql.connect(
    host="monorail.proxy.rlwy.net",
    port=40415,
    user="root",
    password="jAqSZpiuuqXOfyQFbtyMiKDSFwgvrxHd",
    database="biden"
)
cursor = connection.cursor()

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

def insert_data_to_database():
    try:
        item = get_data_from_url()
        # Duyệt qua các mục và chèn dữ liệu vào cơ sở dữ liệu
        for x in item['data']['items']:
            defaultDatasetId = x['defaultDatasetId']
            
            # Câu lệnh SQL để kiểm tra xem defaultDatasetId đã tồn tại trong bảng DatasetStatus chưa
            check_sql = "SELECT COUNT(*) FROM DatasetStatus WHERE dataset_id = %s"
            check_val = (defaultDatasetId,)
            cursor.execute(check_sql, check_val)
            result = cursor.fetchone()  # Lấy kết quả từ truy vấn SELECT

            if result[0] == 0:  # Nếu defaultDatasetId không tồn tại trong bảng
                # Câu lệnh SQL để chèn dữ liệu vào bảng DatasetStatus
                insert_sql = "INSERT INTO DatasetStatus (dataset_id) VALUES (%s)"
                insert_val = (defaultDatasetId,)

                # Thực thi câu lệnh SQL
                cursor.execute(insert_sql, insert_val)
            else:
                # Nếu defaultDatasetId đã tồn tại trong bảng, bỏ qua hoặc ghi log lỗi
                print("defaultDatasetId already exists in table DatasetStatus")
        # Xác nhận các thay đổi
        connection.commit()
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

def call_api():
    print("Task in progress")
    insert_data_to_database()
    try:  
        # Lấy các bản ghi từ cơ sở dữ liệu có status = 0
        sql_select = "SELECT id, dataset_id FROM DatasetStatus WHERE status = 0"
        cursor.execute(sql_select)
        records = cursor.fetchall()

        # Lặp qua từng bản ghi
        for record in records:
            record_id, dataset_id = record

            # Gọi API với default_dataset_id là dataset_id
            url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
            response = requests.get(url)

            # Kiểm tra xem request có thành công không
            if response.status_code == 200:
                # Đổi status của bản ghi từ 0 thành 1
                sql_update = "UPDATE DatasetStatus SET status = 1 WHERE id = %s"
                cursor.execute(sql_update, (record_id,))
                
                for x in response.json():
                    tweet_id = x['id']
                    # Kiểm tra xem tweet_id đã tồn tại trong TweetData hay chưa
                    sql_check = "SELECT COUNT(*) FROM TweetData WHERE tweet_id = %s"
                    cursor.execute(sql_check, (tweet_id,))
                    result = cursor.fetchone()
                    if result[0] == 0:  # Nếu tweet_id chưa tồn tại trong MySQL
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

                        # Tạo câu lệnh SQL để chèn dữ liệu vào bảng
                        sql = "INSERT INTO TweetData (created_at, tweet_id, tweet, likes, retweet_count, user_id, \
                                user_name, user_screen_name,user_description, user_join_date, user_followers_count, \
                                user_location, latitude, longitude, state_1, state_2, country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        val = (created_at, tweet_id, tweet, likes, retweet_count, user_id, user_name, user_screen_name,
                            user_description, user_join_date, user_followers_count, user_location, latitude, longitude, state_1, state_2, country)

                        # Thực thi câu lệnh SQL
                        cursor.execute(sql, val)

                        states = {
                            'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida',
                            'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine',
                            'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska',
                            'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
                            'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas',
                            'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming', 'District of Columbia',
                            'Guam', 'Puerto Rico'
                        }

                        if country == 'United States':
                            if state_1 in states:
                                state_to_send = state_1
                            elif state_2 in states:
                                state_to_send = state_2
                            else:
                                state_to_send = None  # Nếu không có state nào trong danh sách states, gán None

                            # Kiểm tra state_to_send có giá trị không và gửi dữ liệu vào Kafka
                            if state_to_send:
                                send_data_to_kafka({
                                    "created_at": created_at,
                                    "tweet_id": tweet_id,
                                    "tweet": tweet,
                                    "likes": likes,
                                    "retweet_count": retweet_count,
                                    "user_id": user_id,
                                    "user_name": user_name,
                                    "user_screen_name": user_screen_name,
                                    "user_description": user_description,
                                    "user_join_date": user_join_date,
                                    "user_followers_count": user_followers_count,
                                    "user_location": user_location,
                                    "latitude": latitude,
                                    "longitude": longitude,
                                    "state": state_to_send,
                                    "country": country
                                })
                # Lưu các thay đổi vào cơ sở dữ liệu
                connection.commit()

                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(f"Task {record_id} completed at {current_time}")
            else:
                print(f"Failed to call API for record {record_id}. Status code: {response.status_code}")
        print("Task has ended")
    except Exception as e:
        print(f"An error occurred in call_api(): {str(e)}")