import googleapiclient.discovery
import mysql.connector
import pymysql
from sqlalchemy import create_engine
import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import regex as re

#api values of youtube:
api_key='your api key'
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
#channel_id="UCo6N9d1IOIXpzu6keFmK_7A"

#connector connection & engine created :
mydb = mysql.connector.connect(host="localhost",user="root",password="")
#print(mydb)
mycursor = mydb.cursor(buffered=True)
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                        .format(user="root",
                        pw="",
                        db="youtube"))

#to create and use the database in MYSQL database 
mycursor.execute('create database if not exists youtube')
mycursor.execute('use youtube')

#func of channel details from youtube:
def channel_data(channel_id):
    request = youtube.channels().list(
                        part="snippet,contentDetails,Statistics",
                        id= channel_id
    )
    response = request.execute()

    snippet = response['items'][0]['snippet']
    statistics = response['items'][0]['statistics']
    content_details =response['items'][0]['contentDetails']
    
    data={"channel_name": snippet['title'],
        "channel_Id" : channel_id,
        "channel_des": snippet['description'],
        "channel_sub": statistics.get('subscriberCount',0),
        "channel_views": statistics.get('viewCount',0),
        "channel_vic": statistics.get('videoCount',0)
    }

    return data

def channel_pid_data(channel_id):
    request = youtube.channels().list(
                        part="snippet,contentDetails,Statistics",
                        id= channel_id
    )
    response = request.execute()

    content_details =response['items'][0]['contentDetails']
    pid={"channel_pid": content_details['relatedPlaylists']['uploads']}

    return pid["channel_pid"]

#func for no of video ids from Youtube:
def get_video_ids(playlist_id):
    video_ids=[]
    next_page=None
    while True:
        request=youtube.playlistItems().list(part='snippet,contentDetails',
                playlistId=playlist_id,
                maxResults=50,pageToken=next_page)
        response=request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        next_page=response.get('nextPageToken')
        if next_page is None:
            break
    return video_ids

#func of playlist of a channel from Youtube:
def playlist_item(channel_playlistID):
    request=youtube.playlistItems().list(
            part='snippet,contentDetails',
            playlistId=channel_playlistID)
    response=request.execute()
    playlist_Id = response['items'][0]['snippet']['playlistId']
    channel_id = response['items'][0]['snippet']['channelId']
    playlist_name = response['items'][0]['snippet']['title']

    data={"playlist_Id":response['items'][0]['snippet']['playlistId'],
        "channel_id": response['items'][0]['snippet']['channelId'],
        "playlist_name":response['items'][0]['snippet']['title']}
    return data

#convert duration time:(hours,minutes,seconds)
def convert_duration(duration): 
    regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match = re.match(regex, duration)
    if not match:
            return '00:00:00'
    hours, minutes, seconds = match.groups()
    hours = int(hours[:-1]) if hours else 0
    minutes = int(minutes[:-1]) if minutes else 0
    seconds = int(seconds[:-1]) if seconds else 0
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60))


#func of video details for all video_ids from Youtube:
def get_video_info(channel_playlistID,no_of_videos):
    video_data=[]

    for i in no_of_videos:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=i)
        response=request.execute()
        snippet = response['items'][0]['snippet']
        statistics = response['items'][0]['statistics']

        vc_data={
                "video_id" :response['items'][0]['id'],
                "playlist_Id": channel_playlistID,
                "video_name":response['items'][0]['snippet']['title'],                
                "video_description":snippet.get('description',0),
                "published_date":response['items'][0]['snippet']['publishedAt'],
                "View_count":statistics.get('viewCount',0),
                "like_count":response['items'][0]['statistics']['likeCount'],
                "thumbnails":response['items'][0]['snippet']['thumbnails']['default']['url'],
                "duration":convert_duration(response['items'][0]['contentDetails']['duration']),
                "Comment_count":statistics.get('commentCount',0),
                "favorite_count":response['items'][0]['statistics']['favoriteCount'],
                "caption_status":response['items'][0]['contentDetails']['caption']
        }
        video_data.append(vc_data)
    return video_data

#func of comment details from Youtube:
def get_comment_dt(no_of_videos):
    Comment_data=[]
    for video_id in no_of_videos:
        try:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
            )
            response=request.execute()

            for item in response["items"]:
                data=dict(comment_Id=item['snippet']['topLevelComment']['id'],
                        video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
        except:
            pass
    return Comment_data

#create channels,playlists,videos,comments,table:
def create_table():
    create_table_query ='''CREATE TABLE if not exists youtube.channels(channel_name varchar(100),channel_Id varchar(80) primary key,
                        channel_des text,
                        channel_sub int,
                        channel_views int,
                        channel_vic int)'''
    mycursor.execute(create_table_query)

    create_table_query ='''CREATE TABLE if not exists youtube.playlists(playlist_Id varchar(150) primary key,
                        channel_id varchar(150),playlist_name varchar(255)'''
    mycursor.execute(create_table_query)

    create_table_query ='''CREATE TABLE if not exists youtube.videos(video_id varchar(50) primary key,playlist_Id varchar(150),
                    video_name varchar(255),video_description text,published_date timestamp,view_count int,like_count int,
                    thumbnails varchar(200),Duration varchar(30),comment_count int,favorite_count int,caption_status varchar(75),
                    foreign key (playlist_Id) references playlists(playlist_Id)'''
    mycursor.execute(create_table_query)  

    create_table_query='''CREATE TABLE if not exists youtube.comments(comment_Id varchar(150) primary key,video_Id varchar(50),
    comment_Text text,comment_Author varchar(150),comment_Published timestamp'''
    mycursor.execute(create_table_query) 

#Transform corresponding data's into pandas dataframe:
#channel_df=pd.DataFrame(channel_info,index=[0])
#playlists_df=pd.DataFrame(playlists_details,index=[0])
#videos_df=pd.DataFrame(video_details)
#comments_df=pd.DataFrame(Comment_details)

#load the dataframe into channels,playlists,videos & comments table, in SQL Database:
#load channels table:
def load_channel_data(channel_info):
    channel_df=pd.DataFrame(channel_info,index=[0])
    channel_df.to_sql('channels',con=engine,if_exists='append',index=False)
    mydb.commit()

#load playlists table:
def load_playlist_data(playlists_details):
    playlists_df=pd.DataFrame(playlists_details,index=[0])
    playlists_df.to_sql('playlists',con=engine,if_exists='append',index=False)
    mydb.commit()

#load videos table:
def load_videos_data(video_details):
    videos_df=pd.DataFrame(video_details)
    videos_df.to_sql('videos',con=engine,if_exists='append',index=False)
    mydb.commit()

#load comments table:
def load_comments_data(Comment_details):
    comments_df=pd.DataFrame(Comment_details)
    comments_df.to_sql('comments',con=engine,if_exists='append',index=False)
    mydb.commit()

#check whether the channel is already present in table
def channelid_present(channel_id):
    create_table_query = 'SELECT channel_id from channels'
    mycursor.execute(create_table_query)
    column_values = mycursor.fetchall()
    for tb_channel_id in column_values:
        if tb_channel_id[0] == channel_id:
            return True
    return False

#harvest data funtion called through streamlit button:
def harvest_data(channel_id):
    channel_info=channel_data(channel_id)                                 #channel details
    channel_df=pd.DataFrame(channel_info,index=[0])                       #dataframe channel details
    channel_playlistID=channel_pid_data(channel_id)                       #get playlistid  #print(channel_playlistID) #print(channel_playlistID)
    no_of_videos=get_video_ids(channel_playlistID) 
    playlists_details=playlist_item(channel_playlistID)
    playlists_df=pd.DataFrame(playlists_details,index=[0])
    video_details=get_video_info(channel_playlistID,no_of_videos)       #print all video data list
    videos_df=pd.DataFrame(video_details)                               #dataframe for video details 
    Comment_details=get_comment_dt(no_of_videos)                        #func of comment details
    comments_df=pd.DataFrame(Comment_details)                           #dataframe of comment details
    return channel_df,playlists_df,videos_df,comments_df

#setting up streamlit sidebar menu with options:
with st.sidebar:
    selected = option_menu(
        menu_title=None,
        options=['Home','Extract','Analysis'],
        icons=['house','database','file-earmark-bar-graph']
    )

# Setting up the option "Home" in streamlit page:
if selected == 'Home':
    st.title('Welcome to the Youtube Data Analysis Tool')
    st.subheader('Overview')
    st.markdown('''Build a simple dashboard or UI using Streamlit and 
                retrieve YouTube channel data with the help of the YouTube API.
                Stored the data in an SQL database(warehousing),
                enabling querying of the data using SQL''')

# Setting up Extract in streamlit page
if selected == 'Extract':
    st.title('Youtube data Harvesting and Warehousing')
    
    channel_id = st.text_input('Please enter Channel ID')

    harvest_button = st.button('Harvest Data',key="Button1")
    if harvest_button:
        if channel_id:
            c_df,p_df,v_df,cm_df = harvest_data(channel_id)
            st.success('Harvesting Successful')
        else:
            st.warning('Please enter ID')

    load_button = st.button('Load Data',key="Button2")
    if load_button and channel_id:
        channel_id_flag = channelid_present(channel_id)
        if channel_id_flag == False:                                 #if channel data is not present in table already
            channel_info=channel_data(channel_id)
            channel_playlistID=channel_pid_data(channel_id)
            no_of_videos=get_video_ids(channel_playlistID) 
            playlists_details=playlist_item(channel_playlistID)
            video_details=get_video_info(channel_playlistID,no_of_videos)
            Comment_details=get_comment_dt(no_of_videos)
            load_channel_data( channel_info)
            load_playlist_data(playlists_details)
            load_videos_data(video_details)
            load_comments_data(Comment_details)
            st.success('Loading Successful')
        else:
            st.warning('Channel data already present') 
    elif load_button:
        st.warning('Harvest first to load data')

    option = st.radio(label='Please select one from below',options=('Channel data','Playlist data','Video data','Comment data','All data'),index=None)

    if channel_id:
        c_df,p_df,v_df,cm_df = harvest_data(channel_id)
        if option == 'Channel data':
            st.dataframe(c_df)
        elif option == 'Playlist data':
            st.dataframe(p_df)
        elif option == 'Video data':
            st.dataframe(v_df)
        elif option == 'Comment data':
            st.dataframe(cm_df)
        elif option == 'All data':
            st.dataframe(c_df)
            st.dataframe(p_df)
            st.dataframe(v_df)
            st.dataframe(cm_df)

#Function to excute Query of 1st to 10th questions:
def sql_query_1():
    mycursor.execute("""SELECT Distinct c.channel_name, v.video_name FROM youtube.channels c
                    JOIN youtube.playlists p ON c.channel_id = p.channel_id
                    JOIN youtube.videos v ON p.playlist_id = v.playlist_id""")
    out=mycursor.fetchall()
    Q1=pd.DataFrame(out,columns=['channel_name','video_name']).reset_index(drop=True)
    Q1.index +=1
    st.dataframe(Q1)
    #from tabulate import tabulate
    #print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))

def sql_query_2():
    mycursor.execute("""SELECT Distinct c.channel_name, v.video_name FROM youtube.channels c
                    JOIN youtube.playlists p ON c.channel_id = p.channel_id
                    JOIN youtube.videos v ON p.playlist_id = v.playlist_id""")
    out=mycursor.fetchall()
    Q2=pd.DataFrame(out,columns=['channel_name','video_name']).reset_index(drop=True)
    Q2.index +=1
    st.dataframe(Q2)
    #from tabulate import tabulate
    #print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))

def sql_query_3():
    mycursor.execute('''SELECT Distinct c.channel_name, v.video_name,v.view_count FROM youtube.channels c
                    JOIN youtube.playlists p ON c.channel_id = p.channel_id
                    JOIN youtube.videos v ON p.playlist_id = v.playlist_id ORDER BY v.view_count DESC LIMIT 10''')
    out=mycursor.fetchall()
    Q3=pd.DataFrame(out,columns=['Channel_name','video_name','view_count']).reset_index(drop=True)
    Q3.index +=1
    st.dataframe(Q3)
    #from tabulate import tabulate
    #print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))

def sql_query_4():
    mycursor.execute('SELECT comment_count as No_of_comments, video_name as Video_title FROM videos')
    out=mycursor.fetchall()
    Q4=pd.DataFrame(out,columns=['No_of_comments','Video_title']).reset_index(drop=True)
    Q4.index +=1
    st.dataframe(Q4)
    #from tabulate import tabulate
    #print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))

def sql_query_5():
    mycursor.execute('''SELECT c.channel_name,v.video_name AS video_title,v.like_count FROM youtube.channels c 
                    JOIN youtube.playlists p ON c.channel_id = p.channel_id 
                    JOIN youtube.videos v ON p.playlist_id = v.playlist_id ORDER BY v.like_count DESC''')
    out=mycursor.fetchall()
    Q5=pd.DataFrame(out,columns=['channel_name','video_title','like_count']).reset_index(drop=True)
    Q5.index +=1
    st.dataframe(Q5)
    #from tabulate import tabulate
    #print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))

def sql_query_6():
    mycursor.execute('SELECT like_count as No_of_likes, video_name as Video_title FROM videos')
    out=mycursor.fetchall()
    Q6=pd.DataFrame(out,columns=['No_of_likes','Video_title']).reset_index(drop=True)
    Q6.index +=1
    st.dataframe(Q6)
    #from tabulate import tabulate
    #print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))

def sql_query_7():
    mycursor.execute('SELECT channel_views AS Total_Views, channel_name AS Channel_Name FROM channels;')
    out=mycursor.fetchall()
    Q7=pd.DataFrame(out,columns=['Total_Views','Channel_Name']).reset_index(drop=True)
    Q7.index +=1
    st.dataframe(Q7)
    #from tabulate import tabulate
    #print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))

def sql_query_8():
    mycursor.execute('''SELECT channel_name,v.video_name AS Video_title,v.published_date FROM youtube.channels c 
                    JOIN youtube.playlists p ON c.channel_id = p.channel_id 
                    JOIN youtube.videos v ON p.playlist_id = v.playlist_id WHERE YEAR(published_date) = 2022''')
    out=mycursor.fetchall()
    Q8=pd.DataFrame(out,columns=['channel_name','video_title','published_date']).reset_index(drop=True)
    Q8.index +=1
    st.dataframe(Q8)
    #from tabulate import tabulate
    #print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))

def sql_query_9():
    mycursor.execute('''SELECT c.channel_name,TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(v.duration)))), "%H:%i:%s") AS Duration FROM channels c
                    JOIN playlists p ON c.channel_id = p.channel_id
                    JOIN videos v ON p.playlist_id = v.playlist_id GROUP BY p.playlist_id''')
    out=mycursor.fetchall()
    Q9=pd.DataFrame(out,columns=['channel_name','Duration']).reset_index(drop=True)
    Q9.index +=1
    st.dataframe(Q9)
    #from tabulate import tabulate
    #print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))

def sql_query_10():
    mycursor.execute('''SELECT c.channel_name,v.video_name AS video_title,v.comment_count AS No_of_comments FROM youtube.channels c 
                    JOIN youtube.playlists p ON c.channel_id = p.channel_id 
                    JOIN youtube.videos v ON p.playlist_id = v.playlist_id ORDER BY v.comment_count DESC''')
    out=mycursor.fetchall()
    Q10=pd.DataFrame(out,columns=['channel_name','Video_title','No_of_comments']).reset_index(drop=True)
    Q10.index +=1
    st.dataframe(Q10)
    #from tabulate import tabulate
    #print(tabulate(out,headers=[i[0] for i in mycursor.description],tablefmt='psql'))


#setting up the option 'Analysis' in streamlit page:
if selected == 'Analysis':
    st.title('Youtube Data Analysis')
    Query = ['Select your Question',
    '1.What are the names of all the videos and their corresponding channels?', 
    '2.Which channels have the most number of videos, and how many videos do they have?', 
    '3.What are the top 10 most viewed videos and their respective channels?',
    '4.How many comments were made on each video, and what are their corresponding video names?',
    '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7.What is the total number of views for each channel, and what are their corresponding channel names?',
    '8.What are the names of all the channels that have published videos in the year 2022?',
    '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10.Which videos have the highest number of comments, and what are their corresponding channel names?']

    Selected_Query = st.selectbox(' ',options = Query)
    if Selected_Query =='1.What are the names of all the videos and their corresponding channels?':
        sql_query_1()
    if Selected_Query =='2.Which channels have the most number of videos, and how many videos do they have?':
        sql_query_2()
    if Selected_Query =='3.What are the top 10 most viewed videos and their respective channels?':
        sql_query_3()
    if Selected_Query =='4.How many comments were made on each video, and what are their corresponding video names?':
        sql_query_4()
    if Selected_Query =='5.Which videos have the highest number of likes, and what are their corresponding channel names?':
        sql_query_5()  
    if Selected_Query =='6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        sql_query_6()
    if Selected_Query =='7.What is the total number of views for each channel, and what are their corresponding channel names?':
        sql_query_7()
    if Selected_Query =='8.What are the names of all the channels that have published videos in the year 2022?':
        sql_query_8()
    if Selected_Query =='9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        sql_query_9()
    if Selected_Query =='10.Which videos have the highest number of comments, and what are their corresponding channel names?':
        sql_query_10()
