from datetime import date, datetime, timedelta
import os

today = date.today()

bucket_name = "waffleiron"
pages = 3
service_account_key_file = os.path.join(os.environ['HOME'], '.ssh/msds697-waffleiron-38160a8e511f.json')


api = "https://newsnow.p.rapidapi.com/newsv2_top_news_site"

rapidapi_key = "65aeb55cc4msh0c34dd7544eb736p12dae3jsn39fcdba3cd57"
rapidapi_host = "newsnow.p.rapidapi.com"

news = {
    "https://www.cnn.com/politics": "cnn",
    "https://www.foxnews.com/politics": "foxnews"
}