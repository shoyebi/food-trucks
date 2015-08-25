__author__ = 'shoyeb'
from sodapy import Socrata
import psycopg2
from collections import namedtuple

def run_script():
    client = Socrata("data.sfgov.org","wvRAyq5wvCnf9YKGmiuZ7T9y3")
    fetched_data = client.get("/resource/rqzj-sfat.json",select="objectid,latitude,longitude,fooditems,expirationdate")
    for row in fetched_data:
        print str(row)
    print "fetched_data", str(fetched_data)
    data = []

    for i in fetched_data:
        a = Node(i.get('objectid',None),i.get('latitude',None),i.get('longitude',None),i.get('fooditems',None),i.get('expirationdate',None))
        print repr(a)
        data.append(a._asdict())

    print data
    try:
        conn = psycopg2.connect("dbname='mydb'")
    except:
        print "I am unable to connect to the database"
    cur = conn.cursor()
    try:
        cur.executemany("insert into foodtrucks(objectid,latitude, longitude,fooditems,expirationdate) select %(objectid)s,%(latitude)s,%(longitude)s,%(fooditems)s,%(expirationdate)s where not exists (select 1 from foodtrucks where objectid=%(objectid)s)",data)
    except Exception as e:
        print "unable to query postgres =>", e
    conn.commit()



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
    run_script()