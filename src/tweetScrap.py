import pandas as pd 
import snscrape.modules.twitter as sntwitter 
import datetime 
import streamlit as st
from datetime import date
import json
from pymongo import MongoClient

@st.cache_resource
def init_connection():
     try:
        client = MongoClient("mongodb://mongouser:mongouser@ac-5z6nxsr-shard-00-00.fdm7jzb.mongodb.net:27017,ac-5z6nxsr-shard-00-01.fdm7jzb.mongodb.net:27017,ac-5z6nxsr-shard-00-02.fdm7jzb.mongodb.net:27017/?ssl=true&replicaSet=atlas-9uigl4-shard-0&authSource=admin&retryWrites=true&w=majority")
        print("Connected successfully!!!")
        return client
     except Exception as ex:
        print("Could not connect to MongoDB")
        print(ex)
        st.error('Unable to upload.')

#Search parameter function
def search(searchText, since, until): 
    global filename 
    global filename_json
    q = ''
    
    if searchText!='' and since!='' and until!='': 
        q += f"{searchText} since:{datetime.datetime.strftime(since, '%Y-%m-%d')} until:{datetime.datetime.strftime(until, '%Y-%m-%d')}"
        filename = f"{since}_{until}_{searchText}.csv"
        filename_json = f"{since}_{until}_{searchText}.json"        #print(filename) 
    else:
        if searchText!='':
            q += searchText
        if until=='': 
             until = datetime.datetime.strftime(date.today(), '%Y-%m-%d') 
             q += f" until:{until}" 
        if since=='':
            since = datetime.datetime.strftime(datetime.datetime.strptime(until, '%Y-%m-%d') - datetime.timedelta(days=7), '%Y-%m-%d') 
            q += f" since:{since}" 

        q += f"{searchText} since:{datetime.datetime.strftime(since, '%Y-%m-%d')} until:{datetime.datetime.strftime(until, '%Y-%m-%d')}"
        filename = f"{since}_{until}_{searchText}.csv"
        filename_json = f"{since}_{until}_{searchText}.json"
    return q

def convert_df_tocsv(df): 
    if df is not None:  
        with st.spinner('Downloading...'):
           click = st.download_button(label="Download as CSV", data=df.to_csv(index=False).encode('utf-8'), file_name=filename, 
                       mime="text/csv", key='download-csv')
           if click:
               st.success('Downloaded Successfully')

def convert_df_tojson(df):
     if df is not None:  
        with st.spinner('Downloading...'):
            click = st.download_button(label="Download as JSON", file_name=filename_json, mime="application/json", data=json.dumps(df), key='download-json')

            if click:
               st.success('Downloaded Successfully')

def upload_data(df):
    try:
        with st.spinner('Uploading...'):
            client = init_connection()
            # database
            db = client["guvi_social_media"]
            # collection
            db_collection= db["tweeter_data"]

            df.reset_index(inplace=True)
            data_dict = df.to_dict("records")
            db_collection.insert_one({"Scraped Word": searchText, 
                                    "Scrapped Date": datetime.datetime.strftime(date.today(), '%Y-%m-%d'), 
                                    "Scrapped Data":data_dict})
        st.success('Data saved Successfully!')
    except Exception as ex:
        print(ex)
        st.error('Unable to upload.')

def scrap_data():
    #GUI using streamlit
    global searchText
    searchText = st.text_input('Search Text*', '')
    fromDate = st.date_input("From Date*", datetime.date(2023, 1, 1), key="fromDate")
    toDate  = st.date_input("To Date*", datetime.date(2023, 3, 3), key="toDate", max_value=date.today())
    count = st.text_input('Number of tweets (defulat : 500)', '', key="numOfTweets")
    df_result = None

    if st.button('Click to scrap', key="btn_scrap", type="primary"):
        if count == '':
            count = 500
        if searchText!='' and fromDate!='' and toDate!='': 
            st.write('Total number of tweet data ' + str(count) + ' for the text search ' + searchText + ' from ' + datetime.datetime.strftime(fromDate, '%Y-%m-%d') 
                    + ' till ' + datetime.datetime.strftime(toDate, '%Y-%m-%d') + ' will be scrapped from from tweeter applicaiton.')

            q = search(searchText, fromDate, toDate) 
            #st.write('The generated query is ' + q)

            #Creating list to append tweet data  
            tweets_list = [] 

            #Example Query: 'COVID Vaccine since:2021-01-01 until:2021-05-31'
            #declare a username  
            with st.spinner('Loading...'):
                for i,tweet in enumerate(sntwitter.TwitterSearchScraper(q).get_items()): 
                    if i>=int(count): #number of tweets you want to scrape 
                        break 
                    
                    tweets_list.append([datetime.datetime.strftime(tweet.date, '%Y-%m-%d'), tweet.id, tweet.url, tweet.rawContent, tweet.user.username,
                                        tweet.lang, tweet.hashtags, tweet.replyCount, tweet.retweetCount, tweet.likeCount, tweet.source]) 

            st.success('Done!')
            # Creating a dataframe from the tweets list above  
            df_result = pd.DataFrame(tweets_list, columns=['Date', 'TweetId', 'URL', 'Text', 'Username','Language', 'Hashtags','ReplyCount',
                                                            'RetweetCount','LikeCount','Source']) 
            df_result.set_index("Date",inplace=True)
            st.dataframe(df_result)

            #upload_data(tweets_df1, searchText)

            
        else:
            st.write('Invalid input values.')

        return df_result

result = scrap_data()

if result is not None:
    st.button(label="Upload to DB", key="btn_upload", type="secondary", on_click=upload_data, args=(result,))
    #Download Data as CSV
    convert_df_tocsv(result)

    #Download Data as JSON tweets_list
    convert_df_tojson(result.values.tolist()) 
    
    
