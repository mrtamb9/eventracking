import util,logging,resources
logger = logging.getLogger("real_sub_event.py")
logger.setLevel(logging.INFO)
util.set_log_file(logger,resources.LOG_FOLDER+'/real_sub_event.log','w')
class Real_Event_Listener():
    def __init__(self):
        self.date = util.get_today_str()
    
    def start_new_day(self):
        """
        remove all records from sub_table
        """
        print 'being called when real_time events are being called, for example removing data from previous days'
        raise NotImplementedError()
    
    def new_articles_to_event(self,aids,event_id,trend_id):
        """
        a list of articles being added to trend_id. If trend_id = -1, this is a new event (event starts at this day)
        trend_id is trend_id of event_id
        """
        print 'when newe articles being added to event_id'
        raise NotImplementedError()
    def detect_new_event(self,aids,event_id,trend_id):
        """
        cluster a list of articles (all from an event) belonging to trend_id (event )
        trend_id is trend_id of event_id
        """
        print 're-run sub-event for the whole event, like the first time we run'
        raise NotImplementedError()
    def remove_event(self,event_id):
        """
        removing all subs of this event (maybe some event ---> into several smaller even)
        this event_id is removed
        """
        print 'remove the whole event, incase one evetn ---> smaller one, we need to remove all'
        raise NotImplementedError()

class Real_Event_Listenter_Collection():
    def __init__(self):
        self.listeners = []
        
    def add_listener(self,listener):
        self.listeners.append(listener)
    
    def start_new_day(self):
        for i in range(len(self.listeners)):
            listener = self.listeners[i]
            try:
                listener.start_new_day()
            except Exception as e:
                logger.info('ERROR CALLING LISTENER INSTART_NEW_DAY: %d'%i)
                logger.info(str(e))
    def new_articles_to_event(self,aids,event_id,trend_id):
        for i in range(len(self.listeners)):
            listener = self.listeners[i]
            try:
                listener.new_articles_to_event(aids,event_id,trend_id)
            except Exception as e:
                logger.info('ERROR CALLING LISTENER IN NEW_ARTICLES_TO_EVENT: %d'%i)
                logger.info(str(e))
    
    def detect_new_event(self,aids,event_id,trend_id):
        for i in range(len(self.listeners)):
            listener = self.listeners[i]
            try:
                listener.detect_new_event(aids,event_id,trend_id)
            except Exception as e:
                logger.info('ERROR CALLING LISTENER IN DETECT_NEW_EVENT: %d'%i)
                logger.info(str(e))

    def remove_event(self,event_id):
        for i in range(len(self.listeners)):
            listener = self.listeners[i]
            try:
                listener.remove_event(event_id)
            except Exception as e:
                logger.info('ERROR CALLING LISTENER IN REMOVE_EVENT: %d'%i)
                logger.info(str(e))
    
    
    
        