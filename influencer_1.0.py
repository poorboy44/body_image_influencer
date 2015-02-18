fn = 'oscars2014_v3rules_tweets.json'
#fn = 'dove_body_image.txt'
import json
import sys
import pandas as pd 
import codecs
from datetime import datetime
from collections import Counter
data_all = pd.DataFrame()
line_num=0


label_counts = {
    'pos':{'pos':0,'neg':0},
    'neg':{'pos':0,'neg':0}
    }


parent_follower = {} #{'parentID':['followerID_1','followerID_2']}

labels = {}#{'sourceID':{'pos':[timeStamp],'neg':[timeStamp]}}

def add_to_list(mylist,value):
    mylist.append(value)
    return mylist

#build labels data
with codecs.open(fn) as tweets:
    for line in tweets:
        line_num+=1
        try:
            if line !="\n":
                data = json.loads(line)
                #body = data["body"]
                #preferredUsername = data["actor"]["preferredUsername"]
                #displayName = data["actor"]["displayName"]
                sourceID  = data["actor"]["id"].split(":")[2]
                rule_tag =  data["gnip"]["matching_rules"][0]["tag"][:3]
                postedTime = datetime.strptime(data["postedTime"],format="$Y-%m-%dT%H:%M:%S.000Z")
                labels[sourceID]=labels.get(sourceID,{'pos':[],'neg':[]})
                labels[sourceID][rule_tag].append(postedTime)
        except ValueError:
            sys.write.stderr("ValueError on line:{}".format(line_num))
            continue

#build the follower graph
with open('follower_graph.txt','r') as f:
    for row in f.readlines:
        l=row.split('\t')
        parent=l[1]
        follower=l[0]
        parent_follower[parent]=add_to_list(parent_follower.get(parent,[]),follower)


#increments label counts
label_tuple = ('pos','neg')
for source in labels:
    followers = parent_follwer[source]
    for source_label in labels[source]:
        for source_date in labels[source][source_label]:
            for follower in followers:
                for follower_label in labels[follower]:
                    for follower_date in labels[follower][follower_label]:
                        if source_date<follower_date:
                            label_counts[source_label][follower_label]+=1

#print data
print json.dumps(label_counts)

