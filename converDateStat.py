import json
i=12
for line in open("Data/conversa") :
    lastdate = json.loads(line)['context'][-1]['datetime'] 
    year,month=lastdate.split("-")[:2]
    if year=="2017" :
        i=min(i,int(month))
print(i)