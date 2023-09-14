import pandas as pd
import mysql.connector 
import pymongo
from streamlit_option_menu import option_menu
import streamlit as st
from googleapiclient.discovery import build
from dateutil import parser


api_key = "AIzaSyAbiAPxItS6zU3xhGPWFCgE0pqb7UXM1M4"
youtube = build('youtube', 'v3', developerKey=api_key)

client=pymongo.MongoClient('mongodb+srv://mithun25:Nuhtim25@cluster0.zq6xhu7.mongodb.net/')
db=client['youtube']
col=db['channel_details']
coll = db['playlist_details']
col1=db['video_details']
col2=db['comment_details']

mithun=mysql.connector.connect(host='localhost',user='root',password='Nuhtim25*',database='youtube')
suresh=mithun.cursor()

st.set_page_config(
    layout="wide", 
    page_title="YOUTUBE DATA HARVESTING AND WAREHOUSING",
    page_icon="youtube",  
    initial_sidebar_state="expanded",
)
  
selected = option_menu(menu_title="YOUTUBE DATA HARVESTING AND WAREHOUSING",
                           options=["SELECT AND STORE","DATA MIGRATION","DATA ANALYSIS"],
                           menu_icon="youtube",
                           icons=["database-fill-up","caret-right-fill","search"],
                           default_index=0,
                           orientation="horizontal")


def channel_details(channel_id):
    chan_data = []
    response = youtube.channels().list(id=channel_id, part='snippet,contentDetails,statistics').execute()

    for item in response['items']:
        data = {
            'Channel_Id': str(item['id']),
            'Channel_Name': str(item['snippet']['title']),
            'Playlist_Id': str(item['contentDetails']['relatedPlaylists']['uploads']),
            'Subscription_Count': int(item['statistics']['subscriberCount']),
            'Total_Videos': int(item['statistics']['videoCount']),
            'Channel_Views': int(item['statistics']['viewCount']),
            'Channel_Description': str(item['snippet']['description']),
        }
        chan_data.append(data)
    return chan_data
 

def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_Name'])
    return ch_name


def channel_playlist(channel_id):
    video_ids = []
    playlist_data = []
    response = youtube.channels().list(id=channel_id, part='snippet,contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    playlist_response = youtube.playlists().list(id=playlist_id, part='snippet').execute()

    nextToken = None
    for item in response['items']:
        playlist_id = item['contentDetails']['relatedPlaylists']['uploads']
        channel_name = item['snippet']['title']

        for p_item in playlist_response['items']:
            data = {
                'Channel_Name': str(channel_name),
                'Playlist_Id': str(playlist_id),
                'Playlist_Name': str(p_item['snippet'].get('title', ''))
            }
            playlist_data.append(data)

    while True:
        response = youtube.playlistItems().list(playlistId=playlist_id, part='snippet', pageToken=nextToken).execute()

        for i in response['items']:
            video_ids.append(i['snippet']['resourceId']['videoId'])

        nextToken = response.get('nextPageToken')

        if not nextToken:
            break

    return video_ids, playlist_data



def get_video_details(video_ids):
    video_data = []

    for i in range(0, len(video_ids), 50):
        response = youtube.videos().list(part="snippet,contentDetails,statistics",id=','.join(video_ids[i:i+50])).execute() 
        for item in response['items']:
            data = {
                'Channel_Name': str(item['snippet']['channelTitle']), 
                'Channel_Id': str(item['snippet']['channelId']),
                'Video_Name': str(item['snippet']['title']),
                'Video_Id': str(item['id']),  
                'Title': str(item['snippet']['title']),
                'Tags': ','.join(item['snippet'].get('tags', [])),  
                'Thumbnail': str(item['snippet']['thumbnails']['default']['url']),
                'Description': str(item['snippet']['description']),
                'Published_Date': pd.to_datetime(item['snippet']['publishedAt']), 
                'Duration': str(item['contentDetails']['duration']),
                'View_Count': int(item['statistics']['viewCount']), 
                'Like_Count': int(item['statistics'].get('likeCount', 0)),
                'Dislike_Count': int(item['statistics'].get('dislikeCount', 0)),  
                'Comments': int(item['statistics'].get('commentCount', 0)), 
                'Caption_Status': str(item['contentDetails']['caption'])
            }
            video_data.append(data)
    return video_data

def get_comments_details(video_ids):
    comment_data = []
    next_page_token = None
    
    while True:
        try:
            response = youtube.commentThreads().list(part="snippet,replies",videoId=video_ids,maxResults=50,pageToken=next_page_token).execute()
            
            for cmt in response['items']:
                data = {
                    'Comment_Id': str(cmt['id']), 
                    'Video_Id': str(cmt['snippet']['videoId']), 
                    'Comment_Text': str(cmt['snippet']['topLevelComment']['snippet']['textDisplay']),
                    'Comment_Author': str(cmt['snippet']['topLevelComment']['snippet']['authorDisplayName']),
                    'Comment_Posted_Date': str(cmt['snippet']['topLevelComment']['snippet']['publishedAt']),
                    'Like_Count': int(cmt['snippet']['topLevelComment']['snippet']['likeCount']),
                    'Reply_Count': int(cmt['snippet']['totalReplyCount']),
                }
                comment_data.append(data)
                
            next_page_token = response.get('nextPageToken')

            if not next_page_token: 
                break
        except:
             pass

    return comment_data

def upload_data_to_mongodb(channel_ids):
    try:
        for channel_id in channel_ids:
            ch_details = channel_details(channel_id)
            if ch_details:
                col.insert_many(ch_details)
                st.success("Channel details uploaded to MongoDB successfully!")
            else:
                st.warning("No channel details found for the given channel ID.")

            video_ids, playlist_data = channel_playlist(channel_id)
            video_details = get_video_details(video_ids)

            if playlist_data:
                coll.insert_many(playlist_data)
                st.success("Playlist details uploaded to MongoDB successfully!")
            else:
                st.warning("No playlist details found for the selected channel.")

            if video_details:
                col1.insert_many(video_details)
                st.success("Video details uploaded to MongoDB successfully!")
            else:
                st.warning("No video details found for the selected videos.")

            for video_id in video_ids:
                comment_details = get_comments_details(video_id)

                if comment_details:
                    col2.insert_many(comment_details)
                    st.success("Comments uploaded to MongoDB successfully!")
                else:
                    st.warning("No comments found for the selected videos.")

    except Exception as e:
        st.error("Error during data upload to MongoDB:", e)




if selected =="SELECT AND STORE":
        st.markdown("#    ")
        st.write("### Enter YouTube Channel ID below :")
        channel_id = st.text_input("Channel ID").split(',')    

        if channel_id and st.button("Extract Data"):
            ch_details = channel_details(channel_id)
            st.write("#### Extracted data from",ch_details[0]["Channel_Name"],' channel')
            st.table(ch_details)
        
        if st.button("Upload Data to MongoDB"):
             with st.spinner("Uploading data to MongoDB.."):
                 upload_data_to_mongodb(channel_id)

                  

if selected == "DATA MIGRATION":
    st.markdown("#   ")
    st.markdown("###  Youtube channel to Migrate to SQL")
    ch_names = channel_names()
    user_inp = st.selectbox("Select Channel Name", options=ch_names)

    if st.button("Migrate data to MySQL"):
        channel_data = col.find({"Channel_Name": user_inp}, {'_id': 0})
        playlist_data = coll.find({"Channel_Name": user_inp}, {'_id': 0})
        
        video_data = list(col1.find({"Channel_Name": user_inp}, {'_id': 0}))
        video_ids = [video["Video_Id"] for video in video_data]

        comment_data = col2.find({"Video_Id": {"$in": video_ids}}, {'_id': 0})
 
        try:
        
            for channel in channel_data:
                suresh.execute("""
                INSERT INTO channel_db 
                VALUES (%s, %s, %s, %s, %s, %s,%s)
                """, (channel["Channel_Id"], channel["Channel_Name"], channel["Playlist_Id"],
                      channel["Subscription_Count"],channel["Total_Videos"], channel["Channel_Views"], channel["Channel_Description"]))
                
            for playlist in playlist_data:
                suresh.execute("""
                INSERT INTO playlist_db
                VALUES (%s,%s,%s)
                """,( playlist["Channel_Name"],playlist["Playlist_Id"],playlist["Playlist_Name"]))

            for video in video_data:
                suresh.execute("""
                INSERT INTO video_db 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (video["Channel_Name"], video["Channel_Id"], video["Video_Name"], video["Video_Id"], video["Title"],
                      video["Tags"], video["Thumbnail"], video["Description"], video["Published_Date"], video["Duration"],
                      video["View_Count"], video["Like_Count"], video["Dislike_Count"], video["Comments"],
                      video["Caption_Status"]))

            for comment in comment_data:
                suresh.execute("""
                INSERT INTO comment_db 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (comment["Comment_Id"], comment["Video_Id"], comment["Comment_Text"], comment["Comment_Author"],
                      parser.parse(comment["Comment_Posted_Date"]),
                      comment["Like_Count"], comment["Reply_Count"]))


            mithun.commit()
            st.success("Data migration to MySQL completed successfully!")

        except Exception as e:
            st.error("Error during data upload to MySQL:", e)

if selected=="DATA ANALYSIS":
    
    st.write("#### DATA ANALYSIS")
    questions = st.selectbox('Select a Questions',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])    

         

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        suresh.execute("""SELECT Video_Name ,Channel_Name                           
                            FROM video_db
                            ORDER BY Channel_Name""")
        df = pd.DataFrame(suresh.fetchall(),columns=suresh.column_names)
        st.write(df)
                  
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        suresh.execute("""SELECT Channel_Name, Total_Videos
                            FROM channel_db
                            ORDER BY Total_Videos DESC""")
        df = pd.DataFrame(suresh.fetchall(), columns=suresh.column_names)
        st.write(df)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        suresh.execute("""SELECT Video_Name, Channel_Name, View_Count
                        FROM video_db
                        ORDER BY View_Count DESC
                        LIMIT 10""")
        df = pd.DataFrame(suresh.fetchall(), columns=suresh.column_names)
        st.write(df)

    elif questions== '4. How many comments were made on each video, and what are their corresponding video names?':
        suresh.execute("""SELECT Video_Name, COUNT(Comment_id) AS Comment_Count
                        FROM video_db
                        LEFT JOIN comment_db ON video_db.Video_Id = comment_db.Video_id
                        GROUP BY Video_Name
                        ORDER BY Comment_Count DESC""")
        df = pd.DataFrame(suresh.fetchall(), columns=suresh.column_names)
        st.write(df)
    
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        suresh.execute("""SELECT Channel_Name, Video_Name, Like_Count
                        FROM video_db
                        ORDER BY Like_Count DESC
                        LIMIT 10""")
        df = pd.DataFrame(suresh.fetchall(), columns=suresh.column_names)
        st.write(df)
    
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        suresh.execute("""SELECT Video_Name, SUM(Like_Count) AS Total_Likes, SUM(Dislike_Count) AS Total_Dislikes
                        FROM video_db
                        GROUP BY Video_Name""")
        df = pd.DataFrame(suresh.fetchall(), columns=suresh.column_names)
        st.write(df)
    
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        suresh.execute("""SELECT Channel_Name, SUM(View_Count) AS Total_Views
                        FROM video_db
                        GROUP BY Channel_Name
                        ORDER BY Total_Views DESC""")
        df = pd.DataFrame(suresh.fetchall(),columns=suresh.column_names)
        st.write(df)    

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        suresh.execute("""SELECT Channel_Name
                            FROM video_db
                            WHERE Published_Date LIKE '2022%'
                            GROUP BY Channel_Name
                            ORDER BY Channel_Name""")
        df = pd.DataFrame(suresh.fetchall(),columns=suresh.column_names)
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        suresh.execute("""SELECT Channel_Name, AVG(Duration) AS Average_Duration
                        FROM video_db
                        GROUP BY Channel_Name
                        ORDER BY Average_Duration DESC""")    
        df = pd.DataFrame(suresh.fetchall(),columns=suresh.column_names)
        st.write(df)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        suresh.execute("""SELECT  Channel_Name,Video_Name,Comments
                            FROM video_db
                            ORDER BY Comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(suresh.fetchall(),columns=suresh.column_names)
        st.write(df)



    
        

        
    


