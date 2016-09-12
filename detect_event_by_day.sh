#!/bin/bash
# $1 "next"/ or a specific date
set -e
echo "RUN DETECT-EVENT ON: $1"
python data_reader.py $1 $1 # download data from database to update new vocab, document frequency for words
cd bolx
java -cp RunFindEntity.jar:lib/* GetEntityByDay $1 # find entities of data downloaded 
# python e_get_diction.py $1 # update new vocab, document frequency for entities

cd ..
python detec_event_service2.py $1 1 False #find event of date $1 
python detec_trend.py $1 $1 True # connect detected events to previous dates

cd visualize
python hashtag.py $1 $1 # calculate hashtag for events on date

cd ..
python deploy_hier_v2.py $1 $1 # find sub-events for events on date

cd bolx
python run_find_img.py $1 #fix this file: max --> nenxt
python i_event_image.py event $1 # find/update images for event
python i_event_image.py trend $1 # find/update images for trend
python t_hashtag_rank.py $1 #fix this file: max --> nenxt
