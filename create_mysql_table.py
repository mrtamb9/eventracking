import dao,resources,deploy_hier_v2
def create_event_table(table):
    query = '  CREATE TABLE IF NOT EXISTS `%s` (\
    `id` int(11) NOT NULL,\
    `date` date NOT NULL,\
    `article_ids` longtext NOT NULL,\
    `center` mediumtext NOT NULL,\
    `size` int(11) NOT NULL,\
    `img` varchar(100) DEFAULT NULL,\
    `coherence` float DEFAULT NULL,\
    `catid` int(11) DEFAULT \'-1\',\
    `trend_id` int(11) DEFAULT \'-1\',\
    `tag` varchar(800) DEFAULT NULL,\
    `img2` varchar(255) DEFAULT NULL,\
    PRIMARY KEY (`id`),\
    UNIQUE KEY `id_UNIQUE` (`id`),\
    KEY `date` (`date`),\
    KEY `trend_id` (`trend_id`)\
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;'%table
    execute_query(query)

def execute_query(query):
    print 'EXECUTE: %s'%query
    conn = dao.get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    dao.free_connection(conn,cur)

def create_trend_table(table):
    query = 'CREATE TABLE IF NOT EXISTS `%s` (\
    `id` int(11) NOT NULL AUTO_INCREMENT,\
    `start_date` date NOT NULL,\
    `last_date` date NOT NULL,\
    `event_ids` mediumtext NOT NULL,\
    `center` mediumtext NOT NULL,\
    `size` int(11) NOT NULL,\
    `event_num` int(11) NOT NULL,\
    `img` varchar(150) DEFAULT NULL,\
    `coherence` float DEFAULT \'-1\',\
    `catid` int(11) DEFAULT \'-1\',\
    `tag` varchar(200) DEFAULT NULL,\
    PRIMARY KEY (`id`),\
    UNIQUE KEY `id_UNIQUE` (`id`)\
    ) ENGINE=InnoDB AUTO_INCREMENT=22406 DEFAULT CHARSET=utf8;'%table
    execute_query(query)

def create_sub_event_table(table):
    query = 'CREATE TABLE IF NOT EXISTS `%s` (\
    `id` int(11) NOT NULL,\
    `start_date` datetime NOT NULL,\
    `last_date` datetime NOT NULL,\
    `articles` mediumtext NOT NULL,\
    `total_article_num` int(11) NOT NULL,\
    `tag` varchar(300) DEFAULT NULL,\
    `trend_id` int(11) DEFAULT NULL,\
    `coherence` float DEFAULT NULL,\
    `typical_aid` int(11) DEFAULT \'-1\',\
    `group_id` int(11) DEFAULT NULL,\
    PRIMARY KEY (`id`),\
    UNIQUE KEY `id_UNIQUE` (`id`),\
    KEY `trend_id` (`trend_id`),\
    KEY `last_date` (`last_date`),\
    KEY `start_date` (`start_date`)\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;'%table
    execute_query(query)

def main():
    create_event_table(resources.EVENT_TABLE)
    create_event_table(resources.EVENT_CURRENT_TABLE)
    create_trend_table(resources.TREND_TABLE)
    create_sub_event_table(deploy_hier_v2.TABLE_NAME)
    create_sub_event_table(deploy_hier_v2.REAL_TABLE_NAME)
    print 'Finish CREATING ALL TABLES'

if __name__ == '__main__':
    main()



