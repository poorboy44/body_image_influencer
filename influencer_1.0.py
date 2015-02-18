fn = '../dove_body_image.txt'
import json
import sys
#import pandas as pd 
import codecs
from datetime import datetime
#from collections import Counter
#data_all = pd.DataFrame()

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
line_num=0
with codecs.open(fn) as tweets:
    for line in tweets:
        line_num+=1
#        if line_num > 100:
#            break
        try:
            data = json.loads(line)
            #body = data["body"]
            #preferredUsername = data["actor"]["preferredUsername"]
            #displayName = data["actor"]["displayName"]
            sourceID  = int(data["actor"]["id"].split(":")[2])
            rule_tag =  data["gnip"]["matching_rules"][0]["tag"][:3]
            postedTime = datetime.strptime(data["postedTime"], "%Y-%m-%dT%H:%M:%S.000Z")
            labels[sourceID]=labels.get(sourceID,{'pos':[],'neg':[]})
            labels[sourceID][rule_tag].append(postedTime)
        except (ValueError,KeyError):
            sys.stderr.write("Error on line:{}\n".format(line_num))
            continue

#build the follower graph
with open('../follower_graph.txt','r') as f:
    for row in f.readlines():
        l=row.split('\t')
        parent=int(l[1])
        follower=int(l[0])
        parent_follower[parent]=add_to_list(parent_follower.get(parent,[]),follower)


#increments label counts
keyerrors1=0
keyerrors2=0
for source in labels:
    try:
        followers = parent_follower[source]
    except KeyError:
        keyerrors1+=1
        sys.stderr.write("  source: KeyError on {}; total KeyErrors:{}\n".format(source,keyerrors1))
        continue
    for source_label in labels[source]:
        for source_date in labels[source][source_label]:
            for follower in followers:
                try: 
                    for follower_label in labels[follower]:
                        for follower_date in labels[follower][follower_label]:
                            sys.stderr.write("sourceDate:{},syncDate:{}".format(source_date,follower_date))
                            if source_date<follower_date:
                                sys.stderr.write("COUNT IT, label_counts[source_label][follower_label]:".format(label_counts[source_label][follower_label]))
                                label_counts[source_label][follower_label]+=1

                except KeyError:
                    keyerrors2+=1
                    sys.stderr.write("follower: KeyError on {}; total KeyErrors:{}\n".format(follower,keyerrors2))
#print data
with open('influencer_1.0.out','wb') as f:
    f.write(json.dumps(label_counts))

