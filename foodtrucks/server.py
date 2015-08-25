from flask import (Flask,render_template,request)
import os
import psycopg2
from sodapy import Socrata
import psycopg2
from collections import namedtuple
from math import radians, cos
import urlparse

app = Flask(__name__)
app.config['DEBUG'] = os.environ.get('DEBUG', False)
@app.route("/getFoodTrucks",methods=['GET', 'POST'])
def fetch_food_trucks():
    dist = float(request.form.get("distance",1.0))
    message=""
    mylat = float(request.form.get("latitude",37.7728288324927))
    mylon = float(request.form.get("longitude",-122.415671488000))

    lon1 = float(mylon-dist/abs(cos(radians(mylat))*69.0))
    lon2 = float(mylon+dist/abs(cos(radians(mylat))*69.0))
    lat1 = float(mylat-(dist/69.0))
    lat2 = float(mylat+(dist/69.0))

    urlparse.uses_netloc.append("postgres")

    try:
        url = urlparse.urlparse(os.environ["DATABASE_URL"])
        conn = psycopg2.connect(database=url.path[1:],user=url.username,password=url.password,host=url.hostname,port=url.port)
    except Exception as e:
        url = urlparse.urlparse("postgresql://localhost/mydb")
        conn = psycopg2.connect(database=url.path[1:],user=url.username,password=url.password,host=url.hostname,port=url.port)
        print "Unable to connect to db =>", e
    cur = conn.cursor()
    cur.execute("""select objectid, latitude,longitude,3956 * 2 * ASIN(SQRT( POWER(SIN(( %s - cast(latitude as float)) *  pi()/180 / 2), 2) +COS( %s * pi()/180)\
                * COS(cast(latitude as float) * pi()/180) * POWER(SIN(( %s - cast(longitude as float)) * pi()/180 / 2), 2) )) \
                as distance,fooditems,expirationdate from foodtrucks WHERE cast(longitude as float) between %s and %s and \
                cast(latitude as float) between %s and %s """, (mylat, mylat, mylon, lon1, lon2, lat1, lat2))
    rows = cur.fetchall()
    count =0
    data=[]
    data.insert(0,["objectid","latitude","longitude","distance","fooditems","expirationdate"])
    for row in rows:
        data.append(list(row))
        count=count+1
    return render_template("success.html", count_res = str(count),data=data)

@app.route("/")
def home_Page():
    return render_template("home.html")

@app.route("/runScript",methods=['GET', 'POST'])
def run_script():
    client = Socrata("data.sfgov.org","wvRAyq5wvCnf9YKGmiuZ7T9y3")
    fetched_data = client.get("/resource/rqzj-sfat.json",select="objectid,latitude,longitude,fooditems,expirationdate")
    data = []

    for i in fetched_data:
        a = Node(i.get('objectid',None),i.get('latitude',None),i.get('longitude',None),i.get('fooditems',None),i.get('expirationdate',None))
        print repr(a)
        data.append(a._asdict())

    urlparse.uses_netloc.append("postgres")
    try:
        url = urlparse.urlparse(os.environ["DATABASE_URL"])
        conn = psycopg2.connect(database=url.path[1:],user=url.username,password=url.password,host=url.hostname,port=url.port)
    except:
        url = urlparse.urlparse("postgresql://localhost/mydb")
        conn = psycopg2.connect(database=url.path[1:],user=url.username,password=url.password,host=url.hostname,port=url.port)
        print "I am unable to connect to the database"
    cur = conn.cursor()
    try:
        cur.executemany("insert into foodtrucks(objectid,latitude, longitude,fooditems,expirationdate) select %(objectid)s,%(latitude)s,%(longitude)s,%(fooditems)s,%(expirationdate)s where not exists (select 1 from foodtrucks where objectid=%(objectid)s)",data)
    except Exception as e:
        print "unable to query postgres =>", e
    conn.commit()
    return render_template("successScript.html")


class Node(namedtuple('LocRecord', ['objectid','latitude','longitude','fooditems','expirationdate'])):
    def __new__(cls, objectid, latitude='0', longitude='0',fooditems='',expirationdate=''):
        g =  lambda x: ''  if x is None else x
        loc = lambda x: '0'  if x is None else x
        latitude = loc(latitude)
        longitude = loc(longitude)
        fooditems = g(fooditems)
        expirationdate = g(expirationdate)
        return super(Node, cls).__new__(cls, objectid, latitude, longitude,fooditems,expirationdate)



if __name__ == "__main__":
    app.run()