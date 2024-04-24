# Capstone-Project

                      YouTube-Data-Harvesting-and-Warehousing-using-SQL-and-Streamlit


Introduction:
* The project about Building a simple dashboard or UI using Streamlit.
* Retrieve YouTube channel data with the help of  YouTube API.
* Stored the data in SQL database(warehousing),with the help of XAMPP control panel.
* enabling querying of the data using SQL data within the Streamlit.

  
Domain : Social Media


Skills Takeaway :
Python scripting, Data Collection, Streamlit, API integration, Data Management using SQL.


Overview:

Data Harvesting:
* Utilizing the YouTube API to collect data such as video details, channel information, playlists, and comments.

  
Data Storage:
* Setting up a local MySQL database using XAMPP.
* Creating tables to store the harvested YouTube data.
* Using SQL scripts to insert the collected data into the database.

  
Data Analysis:
* Developing a Streamlit application to interact with the SQL database.
  
  
Technology and Tools:
* Python 3.12.2
* XAMPP
* MYSQL
* Youtube API
* Streamlit


Packages and Libraries:


* google-api-python-client        
-> import googleapiclient.discovery        
-> from googleapiclient.errors import HttpError

  
* mysql-connector-python        
-> import mysql.connector

  
* SQLAlchemy                
-> from sqlalchemy import create_engine

  
* pandas        
-> import pandas as pd

  
* streamlit      
-> import streamlit as st
* streamlit_option_menu        
-> from streamlit_option_menu import option_menu
  

Features
Data Collection:
* The data collection process involved retrieving various data points from YouTube using the YouTube Data API. Retrieve channel information, videos details, playlists and comments.

  
Database Storage:
*The collected YouTube data was transformed into pandas dataframes. Before that, a new database and tables were created using the XAMPP control panel. With the help of SQLAlchemy, the data was inserted into the respective tables. The database could be accessed and managed in the MySQL environment provided by XAMPP.


Data Analysis:
* By using YouTube channel data stored in the MySQL database, performed MySQL queries to answer 10 questions about the YouTube channels. When selecting a question, the results will be displayed in the Streamlit application in the form of tables.



