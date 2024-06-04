from sqlalchemy import Column, DateTime, Integer, String, Text
from db.database import Base

class DatasetStatus(Base):
    __tablename__ = 'DatasetStatus'
    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(String(255), nullable=False)
    status = Column(Integer, default=0)

class TweetData(Base):
    __tablename__ = 'TweetData'
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime)
    tweet_id = Column(String(255))
    tweet = Column(Text)
    likes = Column(Integer)
    retweet_count = Column(Integer)
    user_id = Column(String(255))
    user_name = Column(String(255))
    user_screen_name = Column(String(255))
    user_description = Column(Text)
    user_join_date = Column(DateTime)
    user_followers_count = Column(Integer)
    user_location = Column(String(255))
    latitude = Column(String(255))
    longitude = Column(String(255))
    state_1 = Column(String(255))
    state_2 = Column(String(255))
    country = Column(String(255))
    status = Column(Integer, default=0)