CREATE TABLE `event_test` (
`id` int(11) NOT NULL,
`date` date NOT NULL,
`article_ids` longtext NOT NULL,
`center` mediumtext NOT NULL,
`size` int(11) NOT NULL,
`img` varchar(100) DEFAULT NULL,
`coherence` float DEFAULT NULL,
`catid` int(11) DEFAULT '-1',
`trend_id` int(11) DEFAULT '-1',
`tag` varchar(800) DEFAULT NULL,
`img2` varchar(255) DEFAULT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY `id_UNIQUE` (`id`),
KEY `date` (`date`),
KEY `trend_id` (`trend_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `real_event_test2` LIKE `event_test`;
CREATE TABLE `trend_test` (
`id` int(11) NOT NULL AUTO_INCREMENT,
`start_date` date NOT NULL,
`last_date` date NOT NULL,
`event_ids` mediumtext NOT NULL,
`center` mediumtext NOT NULL,
`size` int(11) NOT NULL,
`event_num` int(11) NOT NULL,
`img` varchar(150) DEFAULT NULL,
`coherence` float DEFAULT '-1',
`catid` int(11) DEFAULT '-1',
`tag` varchar(200) DEFAULT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY `id_UNIQUE` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=22406 DEFAULT CHARSET=utf8;
CREATE TABLE `hier_sub_v2` (
`id` int(11) NOT NULL,
`start_date` datetime NOT NULL,
`last_date` datetime NOT NULL,
`articles` mediumtext NOT NULL,
`total_article_num` int(11) NOT NULL,
`tag` varchar(300) DEFAULT NULL,
`trend_id` int(11) DEFAULT NULL,
`coherence` float DEFAULT NULL,
`typical_aid` int(11) DEFAULT '-1',
`group_id` int(11) DEFAULT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY `id_UNIQUE` (`id`),
KEY `trend_id` (`trend_id`),
KEY `last_date` (`last_date`),
KEY `start_date` (`start_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

