fn = 'oscars2014_v3rules_tweets.json'
#fn = 'dove_body_image.txt'
import json
import pandas as pd 
import codecs
data_all = pd.DataFrame()
line_num=1
with codecs.open(fn) as tweets:
	for line in tweets:
		try:
			if line !="\n":
				data = json.loads(line)
				body = data["body"]
				preferredUsername = data["actor"]["preferredUsername"]
				displayName = data["actor"]["displayName"]
				id  = data["actor"]["id"].split(":")[2]
				rule_tag =  data["gnip"]["matching_rules"][0]["tag"][:3]
				postedTime = data["postedTime"]
				#print("\"{}\",\"{}\",\"{}\"\n".format(id, preferredUsername, body))
				data_row = pd.DataFrame([{"user_id":int(id), "preferredUsername":preferredUsername, 
					"displayName":displayName, "body":body, "rule_tag":rule_tag, "postedTime":postedTime}], 
					columns=['user_id', 'preferredUsername', 'displayName', 'body', 'rule_tag', 'postedTime'])
				data_all = data_all.append(data_row)
			line_num +=1
		except:
			#print "error processing line:{}".format(line_num)
			line_num +=1
			continue
follower_graph=pd.read_table('follower_graph.txt', names=['follower_id', 'parent_id'])
users = pd.read_table('oscars2014_v3rules_users.txt', names=['follower_id'])
merged=pd.merge(follower_graph, users)
merged_1 = pd.merge(data_all, merged, left_on=['user_id'], right_on=['parent_id']).loc[:,
	["follower_id", "parent_id", "preferredUsername", "postedTime", "rule_tag", "body"]]
merged_1.rename(columns={'preferredUsername': 'source_un', 'postedTime': 'source_time', 
	'rule_tag':'source_tag', 'body': 'source_body'}, inplace=True)
merged_2 = pd.merge(data_all, merged_1, left_on=['user_id'], right_on=['follower_id']).loc[:,
	["follower_id", "parent_id", "source_un", "source_time", "source_tag", "source_body", 
	"preferredUsername", "postedTime", "rule_tag", "body"]]
merged_2.rename(columns={'preferredUsername': 'sink_un', 'postedTime': 'sink_time', 
	'rule_tag':'sink_tag', 'body': 'sink_body'}, inplace=True)
merged_3 = merged_2.loc[merged_2.loc[:,"source_time"]>merged_2.loc[:, "sink_time"],:]
print (merged_3.loc[:,["source_tag"]].combine_first(merged_3.loc[:,["sink_tag"]]).groupby(['source_tag', 'sink_tag']).size())
merged_3.to_csv('influencer_oscars2014.txt')