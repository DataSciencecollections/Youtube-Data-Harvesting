import googleapiclient.discovery
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st

#to access API
api_key='AIzaSyD2d3_eCadmtjY8U2d6I4IPMe4E-99k1Jo'
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

#get channel_ID
request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id="UC6NK95qassTilrIVwD5oA7Q"
    )
response=request.execute()


#data collection
channel_details=channel_name=(response['items'][0]['snippet']['title'])
published_at=(response['items'][0]['snippet']['publishedAt'])
playlist_id=(response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
subscriber_count=(response['items'][0]['statistics']['subscriberCount'])
view_count=(response['items'][0]['statistics']['viewCount'])
video_count=(response['items'][0]['statistics']['videoCount'])

#to get channels details

def get_channel_details(c_id):
  response=youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=c_id
    ).execute()
  
  for i in response['items']:
    data=dict(channel_name=i['snippet']['title'],
            channel_id=i['id'],
            playlist_id=i['contentDetails']['relatedPlaylists']['uploads'],
            subscriber_count=i['statistics']['subscriberCount'],
            view_count=i['statistics']['viewCount'],
            video_count=i['statistics']['videoCount'])
    return data

#to get video ID
def get_video_id(c_id):
  video_id=[]
  response=youtube.channels().list(id=c_id,
                                  part='contentDetails').execute()
  playlist_id=(response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])

  next_page=None 
    
  while True:
    #get playlist items

    response1=youtube.playlistItems().list(part='snippet',
                                          playlistId=playlist_id,maxResults=50,
                                          pageToken=next_page
                                          ).execute()
    #get video ids
    
    for i in range(len(response1['items'])):
        video_id.append(response1['items'][i]['snippet']['resourceId']['videoId'])

    next_page=response1.get('nextPageToken')

    if next_page is None:
        break
        
  return video_id

#to get video details
def get_video_data(video_Ids):
    video_data=[]
    for video_id in video_Ids:
        request = youtube.videos().list(
                                         part="snippet,contentDetails,statistics",
                                              id=video_id
                                        )


        response=request.execute()

        for item in response['items']:
          data=dict(channel_Name=item['snippet']['channelTitle'],
                     channel_Id=item['snippet']['channelId'],
                     Video_id=item['id'],
                     Title=item['snippet']['title'],
                     Thumbnails=item['snippet']['thumbnails']['default']['url'],
                     Description=item['snippet'].get('description'),
                     Published_at=item['snippet']['publishedAt'],
                     Duration=item['contentDetails']['duration'],
                     View_count=item['statistics'].get('viewCount'),
                     comment_count=item['statistics'].get('commentCount'),
                     like_count=item['statistics'].get('likeCount'),
                     Caption_status=item['contentDetails']['caption'])
                     
          
          video_data.append(data)
            
    return video_data     

#get Playlist details
def get_playlist_details(c_id):
    next_page_token=None
    
    All_data=[]
    while True:
        request=youtube.playlists().list(part="snippet,contentDetails",
                                         channelId=c_id,
                                        maxResults=50,
                                        pageToken=next_page_token)
        response=request.execute()
        
        for item in response['items']:
            data=dict(playlist_id=item['id'],
                      channel_name=item['snippet']['title'],
                      channel_id=item['snippet']['channelId'],
                      published_at=item['snippet']['publishedAt'],
                      title=item['snippet']['title'],
                      video_count=item['contentDetails']['itemCount'])
                      
            
            All_data.append(data)
        next_page_token=response.get('nextPageToken')
        
        if next_page_token is None:
            break
    return All_data

#to get comment details
def get_comment_details(video_ids):
  comment_data=[]
  try:
    for video_id in video_ids:
      request=youtube.commentThreads().list(part="snippet",
                                            videoId=video_id,
                                            maxResults=100

      )
      response=request.execute()

      for item in response['items']:
        data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                  Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                  Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                  Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                  Comment_PublisheAt=item['snippet']['topLevelComment']['snippet']['publishedAt'])
          
        comment_data.append(data)
  except:
    pass
  return comment_data

#conetion for mongodb

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://vaiju:vaiju@cluster0.8myu9x3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

#to create a client of MongoDB
client=pymongo.MongoClient("mongodb+srv://vaiju:vaiju@cluster0.8myu9x3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
#db creation
db=client["Youtube_Data"]

#funtion to get collection
def channel_details(c_id):
    channel=get_channel_details(c_id)
    playlist=get_playlist_details(c_id)
    videos=get_video_id(c_id)
    comment_details=get_comment_details(videos)
    video_details=get_video_data(videos)
    

    col1=db["Data_Details"]
    data=col1.insert_one({"channel_details":channel,
                     "playlist_details":playlist,
                     "video_id":videos,
                     "comments":comment_details,
                     "videos":video_details})
    
    return "successfully uploaded"
 

#to connect with MYSQL server

mydb = mysql.connector.connect(

  host="localhost",

  user="root",

  password="",

  database='Youtube_Data'

)
mycursor=mydb.cursor(buffered=True)

mycursor.execute("use Youtube_Data")


#to create table

def channels_table():
    

        create_query="""CREATE TABLE if not exists channels(channel_name varchar(100),
                                                                channel_id varchar(50)primary key,
                                                                playlist varchar(100),
                                                                subscribers INT,
                                                                views INT,
                                                                videos INT)"""
        mycursor.execute(create_query)
        mydb.commit()



channel_list=[]
db=client["Youtube_Data"]
col1=db["Data_Details"]

#we use this empty{}for find all channel details 

for channel_data in col1.find({},{"_id":0,"channel_details":1}):
   channel_list.append(channel_data["channel_details"])

#to convert datas into dataframe

df=pd.DataFrame(channel_list)

for index,row in df.iterrows():

    query="""insert ignore into channels(channel_name,
                                    channel_id,
                                    playlist,
                                    subscribers,
                                    views,
                                    videos)
                                      
                                    values(%s,%s,%s,%s,%s,%s)"""
    
                                 
    
    values=(row['channel_name'],
            row['channel_id'],
            row['playlist_id'],
            row['subscriber_count'],
            row['view_count'],
            row['video_count'])
    
    result=mycursor.execute(query,values)
    mydb.commit()



#to create playlists table

def playlist_table():

    create_query="""CREATE TABLE if not exists playlists(playlist_id varchar(100)primary key,
                                        channel_name varchar(255),
                                        channel_id varchar(50),
                                        published_at datetime,
                                        title varchar(255),
                                        video_count INT)"""

    mycursor.execute(create_query)
    mydb.commit()

playlist=[]
db=client["Youtube_Data"]
col1=db["Data_Details"]

#we use this empty{} for find all channel details

for pl_data in col1.find({},{"_id":0,"playlist_details":1}):
    for i in range(len(pl_data["playlist_details"])):
        playlist.append(pl_data["playlist_details"][i])

df1=pd.DataFrame(playlist)  

for index,row in df1.iterrows():
    query="""insert ignore into playlists(playlist_id,
                                          channel_name,
                                          channel_id,
                                          published_at,
                                          title,
                                          video_count)

                                          values(%s,%s,%s,%s,%s,%s)"""
    
    values=(row['playlist_id'],
            row['channel_name'],
            row['channel_id'],
            row['published_at'],
            row['title'],
            row['video_count'])
    
    result=mycursor.execute(query,values)
    mydb.commit()


#to create video table

def video_table():

    create_query="""CREATE TABLE if not exists videos(channelname varchar(255),
                                                    channel_id varchar(100),
                                                    video_id varchar(255)primary key,
                                                    title varchar(255),
                                                    thumbnails varchar(255),
                                                    description text,
                                                    published_at datetime,
                                                    duration int,
                                                    views int,
                                                    comments int,
                                                    likes int,
                                                    caption_status varchar(255))"""
                                            
    mycursor.execute(create_query)
    mydb.commit()

video=[]
db=client["Youtube_Data"]
col1=db["Data_Details"]

for video_data in col1.find({},{"_id":0,"videos":1}):
    for i in range(len(video_data["videos"])):
        video.append(video_data["videos"][i])

df2=pd.DataFrame(video)

for index,row in df2.iterrows():
    
    query="""insert ignore into videos(channelname,
                                channel_id,
                                video_id,
                                title,
                                thumbnails,
                                description,
                                published_at,
                                duration,
                                views,
                                comments,
                                likes,
                                caption_status)

                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    
    values=(row['channel_Name'],
            row['channel_Id'],
            row['Video_id'],
            row['Title'],
            row['Thumbnails'],
            row['Description'],
            row['Published_at'],
            row['Duration'],
            row['View_count'],
            row['comment_count'],
            row['like_count'],
            row['Caption_status'])
    result=mycursor.execute(query,values)
    mydb.commit()

#to create comment table

def comment_table():


    create_query="""CREATE TABLE if not exists comment_table(commentid varchar(255),
                                            video_id varchar(100),
                                            comment_text TEXT,
                                            comment_author text,
                                            published_at datetime)"""

    mycursor.execute(create_query)
    mydb.commit()

comments=[]
db=client["Youtube_Data"]
col1=db["Data_Details"]

for com_data in col1.find({},{"_id":0,"comments":1}):
    for i in range(len(com_data["comments"])):
        comments.append(com_data["comments"][i])

df3=pd.DataFrame(comments)

for index,row in df3.iterrows():
    query="""insert into comment_table(commentid,
                                video_id,
                                comment_text,
                                comment_author,
                                published_at)

                                values(%s,%s,%s,%s,%s)"""
                                      
    
    values=(row['Comment_Id'],
            row['Video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_PublisheAt'])
            
    
    result=mycursor.execute(query,values)
    mydb.commit()

#for table creation
    
def tables():
    channels_table()
    playlist_table()
    video_table()
    comment_table()

    return "tables uploded"

#steamlit dataframe for channels

def view_channels_table():
   channel_list=[]
   db=client["Youtube_Data"]
   col1=db["Data_Details"]

   for channel_data in col1.find({},{"_id":0,"channel_details":1}):
      channel_list.append(channel_data["channel_details"])

   df=st.dataframe(channel_list)

   return df

#streamlit dataframe for playlists table

def view_playlists_table():
    playlist=[]
    db=client["Youtube_Data"]
    col1=db["Data_Details"]

    for pl_data in col1.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pl_data["playlist_details"])):
            playlist.append(pl_data["playlist_details"][i])

    df1=st.dataframe(playlist)

    return df1

#streamlit dataframe for videos

def view_video_table():
    video=[]
    db=client["Youtube_Data"]
    col1=db["Data_Details"]

    for video_data in col1.find({},{"_id":0,"videos":1}):
        for i in range(len(video_data["videos"])):
            video.append(video_data["videos"][i])

    df2=st.dataframe(video)
    return df2


#streamlit dataframe for comments

def view_comment_table():
    comments=[]
    db=client["Youtube_Data"]
    col1=db["Data_Details"]

    for com_data in col1.find({},{"_id":0,"comments":1}):
        for i in range(len(com_data["comments"])):
            comments.append(com_data["comments"][i])

    df3=st.dataframe(comments)

    return df3

#streamlit data generation

with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skills Takeaway")
    st.caption("Python scripting")
    st.caption("Data Collection")
    st.caption("Streamlit")
    st.caption("API integration")
    st.caption("API integration")



#SQL connection

mydb = mysql.connector.connect(

  host="localhost",

  user="root",

  password="",

  database='Youtube_Data'

)
mycursor=mydb.cursor(buffered=True)

mycursor.execute("use Youtube_Data")

question=st.selectbox("SELECT YOUR QUESTIONS",("1.Total Video and Channel name",
                                               "2.Channels with most number of videos",
                                               "3.Top 10 viewed videos",
                                               "4.Comments for Videos",
                                               "5.Which video get highest likes",
                                               "6.likes of all videos",
                                               "7.channels views",
                                               "8.which videos published in the year of 2022",
                                               "9.Average duration for all videos in each channel",
                                               "10.Which video get highest number of comments"))


mydb = mysql.connector.connect(

  host="localhost",

  user="root",

  password="",

  database='Youtube_Data'

)
mycursor=mydb.cursor(buffered=True)

mycursor.execute("use Youtube_Data")


if question=="1.Total Video and Channel name":
    query1="""slect Title as videos,channel_Name as channelname from videos"""
    mycursor.execute(query1)
    mydb.commit()
    table1=mycursor.fetchall()
    df1=pd.DataFrame(table1,columns=["Title_of_video","channel_name"])
    st.write(df1)

elif question=="2.Channels with most number of videos":
    query2="""select channel_Name as channelname,video_count as no of videos from channels
              order by video_count desc"""
    mycursor.execute(query2)
    mydb.commit()
    table2=mycursor.fetchall()
    df2=pd.DataFrame(table2,columns=["channel_name","no of videos"])
    st.write(df2)

elif question=="3.Top 10 viewed videos":
    query3="""select View_count as views,channel_Name as channelname,Title as videotitle from videos
               where View_count is not null order by desc limit 10"""
    mycursor.execute(query3)
    mydb.commit()
    table3=mycursor.fetchall()
    df3=pd.DataFrame(table3,columns=["views","channel_name","videotitle"])
    st.write(df3)

elif question=="4.Comments for Videos":
    query4="""select comment_count as comments,Title as videotitle from videos where comment_count is not null"""
    mycursor.execute(query4)
    mydb.commit()
    table4=mycursor.fetchall()
    df4=pd.DataFrame(table4,columns=["comments","videotitle"])
    st.write(df4)

elif question=="5.Which video get highest likes":
    query5="""select Title as videotitle,channel_Name as channelname,like_count as likes
               where like_count is not null order by desc"""
    mycursor.execute(query5)
    mydb.commit()
    table5=mycursor.fetchall()
    df5=pd.DataFrame(table5,columns=["videotltle","channel_name","likes"])
    st.write(df5)

elif question=="6.likes of all videos":
    query6="""select like_count as likecount,Title as videotitle from videos"""
    mycursor.execute(query6)
    mydb.commit()
    table6=mycursor.fetchall()
    df6=pd.DataFrame(table6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7.channels views":
    query7="""select channel_name as channelname,views as viewcount from channels"""
    mycursor.execute(query7)
    mydb.commit()
    table7=mycursor.fetchall()
    df7=pd.DataFrame(table7,columns=["channelname","viewcount"])
    st.write(df7)

elif question=="8.which videos published in the year of 2022":
    query8="""select Title as videotitle,Published_at as publisheddate,channel_Name as channelname from videos
               where extract(year from Published_at)=2022"""
    mycursor.execute(query8)
    mydb.commit()
    table8=mycursor.fetchall()
    df8=pd.DataFrame(table8,columns=["video_title","published_date","channel_name"])
    st.write(df8)

elif question=="9.Average duration for all videos in each channel":
    query9="""select channel_Name as channelname,AVG(Duration) as averageduration from videos group by channel_Name"""
    mycursor.execute(query9)
    mydb.commit()
    table9=mycursor.fetchall()
    df9=pd.DataFrame(table9,columns=["channelname","avg_duration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_name=row["channelname"]
        average_duration=row["avg_duration"]
        avg_str=str( average_duration)
        T9.append(dict(channeltitile=channel_name,avgduration=avg_str))
        
        df1=pd.DataFrame(T9)
        st.write(df1)

elif question=="10.Which video get highest number of comments":
    query10="""select Title as videotitle,channel_name as channelname,comment_count as comments from videos
               where comment_count is not null order by comment_count desc"""
    mycursor.execute(query10)
    mydb.commit()
    table10=mycursor.fetchall()
    df10=pd.DataFrame(table10,columns=["video_title","channel_name","comments"])
    st.write(df10)





    
       
           
            
    
    






    
  





 




