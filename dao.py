import pymysql
import resources
ACC_PATH = resources.DB_CONFIG_PATH
NEWS_TABLE = 'NewsDb.news'
TOKEN = 'vntokenizer'
DOMAIN = 'source'
CREATE_TIME = 'create_time'
GET_TIME = 'get_time'
CATID = 'catid'
ID = 'id'
CONTENT = 'content'
URL  = 'url'
TITLE = 'title'
CATID = 'catid'
DESCRIPTION = 'description'
TAGS='tag'
def get_connection():
    f = open(ACC_PATH,'r')
    khost = f.readline().strip()
    #print 'host: %s'%khost
    kdb = f.readline().strip()
    #print 'db: %s'%kdb
    kuser = f.readline().strip();
    #print 'user: %s'%kuser
    kpasswd = f.readline().strip();
    #print 'password: %s'%kpasswd
    f.close()
    conn = pymysql.connect(host=khost, user=kuser, passwd=kpasswd, db=kdb,charset='utf8')
    return conn
def get_cursor(conn):
    cur = conn.cursor()
    return cur

#cur.execute("SELECT Host,User FROM user")

def display_cursor(cur):
    print(cur.description)
    for row in cur:
        print(row)
def free_connection(conn,cur):
    cur.close()
    conn.close()
