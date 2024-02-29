from datetime import date, datetime, timedelta
import os

today = date.today()

bucket_name = "waffleiron"

service_account_key_file = "msds697-waffleiron-38160a8e511f.json"

url = "https://newsnow.p.rapidapi.com/newsv2_top_news_site"

rapidapi_key = "65aeb55cc4msh0c34dd7544eb736p12dae3jsn39fcdba3cd57"
rapidapi_host = "newsnow.p.rapidapi.com"

news = {
    "https://www.cnn.com/politics": "cnn",
    "https://www.foxnews.com/politics": "foxnews"
}