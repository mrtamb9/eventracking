import dao
TABLE_NAME = 'local_event_trend'
ID = 'id'
TREND_ID = 'trend_id'
EVENT_ID = 'event_id'
CONTEXT_SIM = 's1'
ENTIRY_SIM = 's2'
FINAL_SCORE = 'fs'
GAP_DATE = 'gap_date'
EVENT_SIZE ='event_size'
LAST_SIZE = 'last_size'
CATID = 'catid'
CLOSENESS = 'closeness'
def create_insert_query(record):
    query = 'insert into %s(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) values(%d,%f,%f,%d,%d,%f,%d,%d,%d,%f)'%(TABLE_NAME,\
    CATID,CONTEXT_SIM,ENTIRY_SIM,EVENT_ID,EVENT_SIZE,\
    FINAL_SCORE,GAP_DATE,LAST_SIZE,TREND_ID,\
    CLOSENESS,\
    record[CATID],record[CONTEXT_SIM],record[ENTIRY_SIM],record[EVENT_ID],record[EVENT_SIZE],\
    record[FINAL_SCORE],record[GAP_DATE],record[LAST_SIZE],record[TREND_ID],\
    record[CLOSENESS])
    return query

def insert_records(records):
    return 
    print 'insert: %d records to event_trend table'%(len(records))
    conn = dao.get_connection()
    cur = conn.cursor()
    for record in records:
        query = create_insert_query(record)
        cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)