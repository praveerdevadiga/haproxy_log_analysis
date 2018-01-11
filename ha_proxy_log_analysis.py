import numpy as np
import pandas as pd
import sys, os
import re 
import csv
import shlex
import json
import datetime
import glob, os

pattern_json = re.compile(r'{.*}')
pattern_select_words = re.compile(r'[^:,{}]+')
pattern_method = re.compile(r'(^[A-Z]+\b)')
pattern_url = re.compile(r'([/bh].*)\s')
pattern_protocol = re.compile(r'(\s[H].*)')

month = input("Enter the month : ")
day = input("Enter the date : ")

date = '2017-'+str(month)+'-'+str(day)

haproxy = ['APP_18.194.19.171', 'PMS_35.158.197.58', 'WWW_35.156.87.211']

for proxy in haproxy:

	os.chdir('/home/tech/haproxy_log_analysis/'+proxy+'/logs')
	content = []

	for logfiles in glob.glob('haproxy.*'):
	    with open(logfiles) as file:
	        for line in file:
	            for match in re.findall(pattern_json, line):
	                string_json = match
	            string_json = re.sub(pattern_select_words, r'"\g<0>"', string_json)
	            try:
	                json_data = json.loads(string_json)
	            except ValueError:
	                continue
	            data = []
	            timestamp = int(json_data.get('timestamp'))
	            day = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')

	            if day == date:
	                data.append(day)
	                data.append(datetime.datetime.fromtimestamp(timestamp).strftime('%H:%M:%S'))

	                http_request = str(json_data.get('http_request',None))
	                for url in re.findall(pattern_url, http_request):
	                    data.append(url)
	                for method in re.findall(pattern_method, http_request):
	                    data.append(method)
	                for protocol in re.findall(pattern_protocol, http_request):
	                    data.append(protocol)

	                data.append(json_data.get('upstream_addr'))
	                data.append(json_data.get('remote_addr'))
	                data.append(int(json_data.get('http_status')))
	                data.append(int(json_data.get('session_duration')))
	                data.append(int(json_data.get('upstream_connect_time')))
	                data.append(int(json_data.get('upstream_response_time')))
	            
	                content.append(data)

	columns = ['day', 'time', 'url', 'method', 'protocol', 'upstream_addr',
	           'remote_addr', 'http_status', 'session_duration',
	           'upstream_connect_time', 'upstream_response_time']

	df = pd.DataFrame(content, columns=columns)
	df['UPST_min']=(df.upstream_response_time)/(1000*60)
	del df['upstream_response_time']
	df['upstream_response_time'] = pd.to_timedelta((60*df['UPST_min']), unit='s')
	del df['UPST_min']
	df.to_csv('/home/tech/haproxy_log_analysis/'+proxy+'/'+proxy[0:3]+'_merged_logs_'+date+'.csv', index = None)

	new = df.groupby(['url','upstream_addr','method'])['upstream_response_time'].agg(['sum','count'])
	n=new.reset_index()
	n.rename(columns={'count':'hits','sum':'UPST_RT_duration'},inplace=True)
	a=n.reset_index()
	del a['index']
	a1 = a.sort_values(by=['hits'], ascending=False)
	a1 = a1.head(100)
	a1['UPST_RT_avg']=(a1.UPST_RT_duration)/(a1.hits)
	a1 = a1.reset_index()
	del a1['index']

	l = []
	l = a1.url
	sd = []
	p = []
	for i in l:
	    a = df[df.url == i]
	    x = np.std(a['upstream_response_time'])
	    sd.append(x)
	    p1 = np.percentile(a['upstream_response_time'],90)
	    p.append(p1)
	sd_array = pd.Series(sd)
	p_array = pd.Series(p)
	a1['SD'] = sd_array.values
	a1['90_percentile'] = p_array.values
	a1.to_csv('/home/tech/haproxy_log_analysis/'+proxy+'/'+proxy[0:3]+'_haproxy_log_report_urlwise_hits_'+date+'.csv', index = None)
	print('Analysis report for '+proxy[0:3]+'-HAProxy is created\n')