# Twitter json parser
# This is built to create networks that can be viewed in Gephi, and x/y maps for use in QGIS
# As such, it may be idiosyncratic and not suitable for all purposes


#some code from http://mike.teczno.com/notes/streaming-data-from-twitter.html
#from http://stackoverflow.com/questions/9942594/unicodeencodeerror-ascii-codec-cant-encode-character-u-xa0-in-position-20/9942822
# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import json
import csv

tweets = []
errors = []


if len(sys.argv) > 1:
    line_generator = open(sys.argv[1])
    outputFileName = "%s.csv" % (sys.argv[1])
    bigfile = open(outputFileName, 'w')
    geoOutputFileName = "%s-Geo.csv" % (sys.argv[1])
    geofile = open(geoOutputFileName, 'w')
	
else:
    line_generator = sys.stdin
    bigfile = open('output.csv', 'w')
    geofile = open('geooutput.csv', 'w')

bigfile.writelines ("source,target,connection_type,created_at,tweet_id")
bigfile.writelines('\n')

geofile.writelines ("tweet_id,lat,lon,created_at,text")
geofile.writelines('\n')


for line in line_generator:
    # catch any broken tweets - we have many # on large files #
    # writing to the file as we go as we keep running out of memory #
	try:
		tweet = json.loads(line)
		print(tweet)
			#error checking as some tweets could be broken #
		try:
			if tweet['entities']['hashtags']:
				tweetend = tweet['entities']['hashtags']
				for hashtag in tweetend:
					bigfile.writelines("@%s,#%s,hashtag,%s,%s" %(str(tweet['user']['screen_name']).lower(),str(hashtag['text']).lower(),tweet['created_at'],tweet['id_str']))
					bigfile.writelines('\n')
		except:
			errors.append(tweet['id_str'])
	
		#error checking as some tweets could be broken #
		try:
			if tweet['entities']['user_mentions']:
				tweetend = tweet['entities']['user_mentions']
				for mention in tweetend:
					bigfile.writelines("@%s,@%s,mention,%s,%s" %(str(tweet['user']['screen_name']).lower(),str(mention['screen_name']).lower(),tweet['created_at'],tweet['id_str']))
					bigfile.writelines('\n')

		except:
			errors.append(tweet['id_str'])
		
		try:
			if tweet['geo']:
				lats = tweet['geo']['coordinates'][0]
				lons = tweet['geo']['coordinates'][1]
				geofile.writelines('%s,%s,%s,%s,"%s"' %(tweet['id_str'],lats,lons,tweet['created_at'],tweet['text']))
				geofile.writelines('\n')
				
		except:
			pass

	except:
		pass

bigfile.close()	
		
if len(errors) > 0:
	with open('errors.csv', 'wb') as myfile:
			wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
			wr.writerow(errors)

