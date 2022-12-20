import requests
from datetime import datetime, timezone
import json
import boto3
from os import listdir
from os.path import isfile, join

s3_client = boto3.client("s3")
S3_BUCKET = "redditoutput"  
local = '/tmp'

def _get_key():
    dt_now = datetime.now(tz=timezone.utc)
    KEY = (
        dt_now.strftime("%Y-%m-%d")
        + "-"
        + dt_now.strftime("%H")
        + "-"
        + dt_now.strftime("%M")
        
    )
    return KEY


def get_data(object_type):
	submissions = {'post_score' : [],
			   'post_author' : [],
			   'post_id' : [],
			   'post_permalink' : [],
			   'post_date' : [],
			   'post_body' : []
	}
	
	comments = { 'com_parent_id' : [],
			'com_permalink' : [],
			'com_body': [],
			'com_date': [],
			'com_author': [],
			'com_score': []
	}

	subreddit = "wallstreetbets"  # put the subreddit you want to download in the quotes

	filter_string = f"subreddit={subreddit}"
	start_hour = 1 #limit the range only to pull data from the last 24 hours 
	step = 1 #changing the step by hour or we can increase it by any hour amount
	max_hours = 12 # set max hour of visibility, we use 12 as in 12 hours

	for x in range(1,max_hours + 1, step):
		#initializing the start and end of url 
		start_rate = '{}h'.format(str(start_hour))
		end_rate = '{}h'.format(str(start_hour-step))

		#format the url
		url = "https://api.pushshift.io/reddit/{}/search?limit=1000&sort=desc&{}&after={}&before={}"
		url = url.format(object_type, filter_string, start_rate, end_rate)
	
		#send request, we also used a reddit account called /u/Watchful1
		json_text = requests.get(url, headers={'User-Agent': "Post downloader by /u/Watchful1"})

		# time.sleep(0.3)  # pushshift has a rate limit, if we send requests too fast it will start returning error messages
		
		#verify that the data returned is a json
		try:
			json_data = json_text.json()
		except json.decoder.JSONDecodeError:
			break
		
		#verify that we have data field within the json object
		if 'data' not in json_data:
			break
		else:
			objects = json_data['data']

		#verify there is more than 0 data returned
		if len(objects) == 0:
			break
		else:
			for object in objects:
				if object_type == 'comment': 
					try:
						comments['com_score'].append(str(object['score']))
						comments['com_permalink'].append(str(object['permalink'])) #link
						comments['com_author'].append(str(object['author'])) #author
						comments['com_parent_id'].append(str(object['parent_id'])) #i think submissions id might want to remove the "t3_"
						comments['com_date'].append(datetime.fromtimestamp(object['created_utc']).strftime("%Y-%m-%d"))
						comments['com_body'].append(object['body'].encode(encoding='ascii', errors='ignore').decode())
					except Exception as err:
						pass
				elif object_type == 'submission':
					if object['is_self']:
						if 'selftext' not in object:
							continue	
						try:
							submissions['post_score'].append(str(object['score'])) #score of the post
							submissions['post_author'].append(str(object['author'])) #post author
							submissions['post_id'].append(str(object['id'])) #submission id
							submissions['post_permalink'].append(str(object['permalink'])) #
							submissions['post_date'].append(datetime.fromtimestamp(object['created_utc']).strftime("%Y-%m-%d"))
							submissions['post_body'].append(object['selftext'].encode(encoding='ascii', errors='ignore').decode())

						except Exception as err:
							pass
				else:
					pass
		start_hour += step
	#return the desired object
	if object_type == 'comment':
		return comments
	elif object_type == 'submission':
		return submissions

def write_to_local(data, part, loc=local):
	key = _get_key()
	file_name = loc + "/" + str(part) + "-" + key + ".json"
	json_string = json.dumps(data)
	with open(file_name, "w") as file:
		json.dump(json_string, file)
	return file_name

def lambda_handler(event=None, context=None):
	# return_statement = get_data("submission")
	#get submission data 
	sub_data = get_data("submission")
	sub_name = write_to_local(sub_data, "submission")
	key = _get_key()
	
	#get comment data 
	com_data = get_data("comment")
	com_name = write_to_local(com_data, "comment")
	# com_key = _get_key("comment")
	
	files = [f for f in listdir(local) if isfile(join(local, f))]
	for f in files:
		s3_client.upload_file(local+ "/" + f, S3_BUCKET, f)

