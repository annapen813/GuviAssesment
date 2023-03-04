Python-Project-Scrape-Twitter-Data

Steps To Follow
Run the required dependencies given in the requirementALL.txt

Clone your mmaster repository to your local machine. The github repository URL is .

Start the tweetScrap.py file, using the command "streamlit run tweetScrap.py".

This will initialize the Streamlit form with the cusotm port of http://localhost:8051

It has below fields
    Search Text
    From Date
    To Date
    Number of tweets

   Search Text - Text to scrap the data from tweeter using snScrape
   From Date - From which date you want to scrape
   To Date - Till which date you want to scrape
   Number of tweets - Count to define the number of scrape data to be fetched, by default its defined as 500

Once you click the button "Click to Scrap", the behid method will fetch the data from tweeter and displayed in the dataframe

We also give 3 options to store the data
    Upload it to Mongo DB
    Download the data as CSV file
    Download the data as JSON file

By clicking on the respective buttons, the respective methods will be triggered and the data will be saved or downloaded on your local machine



