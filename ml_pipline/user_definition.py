from datetime import date, datetime, timedelta
import os

today = date.today()

bucket_name = "waffleiron"
pages = 10
service_account_key_file = os.path.join(os.environ['HOME'], 'airflow/dags/msds697-waffleiron-38160a8e511f.json')
#service_account_key_file = os.path.join(os.environ['HOME'], '.ssh/msds697-waffleiron-38160a8e511f.json')


api = "https://newsnow.p.rapidapi.com/newsv2_top_news_site"
uri = "mongodb+srv://waffleiron:zNkFSHTfTGroBXRq@waffleironcluster.7uzj7t2.mongodb.net/?retryWrites=true&w=majority&appName=WaffleIronCluster"

rapidapi_key = "65aeb55cc4msh0c34dd7544eb736p12dae3jsn39fcdba3cd57"
rapidapi_host = "newsnow.p.rapidapi.com"

news = {
    "https://www.cnn.com/politics": "cnn",
    "https://www.foxnews.com/politics": "foxnews",
    "https://www.bbc.com/news/politics": "bbc",
    # "https://www.usatoday.com/news/politics/": "usatoday", ## not work for usatoday
    "https://www.washingtonpost.com/politics/":"washingtonpost",
    "https://www.wsj.com/politics" : "wsj",
    "https://www.nytimes.com/section/politics" :"nytimes",
    "https://www.theguardian.com/politics" : "theguardian",
    "https://www.cbc.ca/news/politics": "cbc"
}