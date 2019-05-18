from flask import Flask,jsonify,request
import ast
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
spark = SparkSession.builder.appName("Tweets Analysis").getOrCreate()
import pandas as pd

labels = []
values = []

def load_data(filename, cols):
    csv_data = pd.read_csv(filename, sep=',', usecols=cols)
    return csv_data

app = Flask(__name__)
CORS(app)

@app.route('/api/countries')
def tweets_by_countries():
    data = load_data('output/GlobalTweets/filteredCountry.csv', ['name', 'id','value'])
    return data.to_json(orient='records')

@app.route('/api/events')
def tweets_by_events():
    data = load_data('output/EventTweets/Eventwisetweets.csv', ['country','total'])
    return data.to_json(orient='records')

@app.route('/api/phone')
def tweets_by_phones():
    data = load_data('output/DeviceTweets/filteredDevice.csv', ['Country', 'Iphone', 'Android','Total'])
    return data.to_json(orient='records')



@app.route('/api/hashtags')
def tweets_by_hashtags():
    data = load_data('output/HashtagsTweets/TrendingHashtag.csv', ['Hashtags','Frequency'])
    return data.to_json(orient='records')


@app.route('/api/language')
def tweets_by_language():
    data = load_data('output/LanguageTweets/language.csv', ['language', 'total'])
    return data.to_json(orient='records')

@app.route('/api/retweets')
def tweets_by_retweets():
    data = load_data('output/ReTweets/RetweetsHigh.csv', ['ScreenName','RetweetCount','ImageUrl'])
    return data.to_json(orient='records')

@app.route('/api/verified')
def tweets_by_verified():
    data = load_data('output/VerifiedTweets/filteredVerified.csv', ['Verified', 'Nonverified','Country'])
    return data.to_json(orient='records')

@app.route('/api/iplteams')
def tweets_by_iplteams():
    data = load_data('output/IPLTweets/IPLTeamsTweets.csv', ['Team', 'Count','URL'])
    return data.to_json(orient='records')

@app.route('/api/alerts')
def tweets_by_alerts():
    data = load_data('output/AlertTweets/AlertTwitter.csv', ['Country', 'Totalalerts'])
    return data.to_json(orient='records')

@app.route('/api/textanalysis')
def tweets_by_textanalysis():
    data = load_data('output/TextTweets/TextAnalysisTweets.csv', ['Length','Lessthan50','Between','Greater100'])
    return data.to_json(orient='records')

@app.route('/api/covertcsvtojson')
def csv_to_json():
    data = load_data('output/GlobalTweets/filteredCountry.csv',['name','id','value'])
    return data.to_json(orient='records')


@app.route('/updateData', methods=['POST'])
def update_data_post():
    global labels, values
    if not request.form or 'data' not in request.form:
        return "error",400
    labels = ast.literal_eval(request.form['label'])
    values = ast.literal_eval(request.form['data'])
    print("labels received: " + str(labels))
    print("data received: " + str(values))
    return "success",201


@app.route('/refreshData')
def refresh_graph_data():
    global labels, values
    print("labels now: " + str(labels))
    print("data now: " + str(values))
    return jsonify(sLabel=labels, sData=values)

if __name__ == '__main__':
    app.run()
