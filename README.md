# Youtube_data_harvesting

Problem Statement: The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.

NAME :MITHUN S

The application should have the following features:

*YouTube API: You'll need to use the YouTube API to retrieve channel and video data. You can use the Google API client library for Python to make requests to the API.

1. Input a YouTube channel ID and retrieve all the relevant data (channel_id,channel_name,playlist_id,subcription_count,total_videos,channel_views,Description) using Google API.

2. Option to store the data in a MongoDB database. Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button. Option to select a channel name and migrate its data from the data lake to a SQL database as tables.

3. Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

4. Migrate data to a SQL data warehouse: After you've collected data for multiple channels, you can migrate it to a SQL data warehouse. You can use a SQL database such as MySQL for this.

5. Display data in the Streamlit app: Finally, you can display the retrieved data in the Streamlit app. Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.

Configuration:

1.Open the data.py file in the project directory.

2.Set the desired configuration options:

3.Specify your YouTube API key.

4.Choose the database connection details (SQL and MongoDB).

5.Get the Youtube Channel ID

6.provide the Youtube Channel ID data to be harvested.

7.Set other configuration options as needed.

Usage:

1.Launch the Streamlit app: streamlit run data.py

2.Run the data.py script, make sure you have main and sql files in the same folder.

3.The app will start and open in your browser. You can explore the harvested YouTube data and visualize the results.

