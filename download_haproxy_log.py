import glob, os
import tarfile


def download_zip_logs(ip_num, start_log_file, end_log_file):
	for log_file_number in range(start_log_file,end_log_file+1):
		os.system('scp  -i .ssh/aws-prod_ff.pem ubuntu@'+ip_num+':/var/log/haproxy/haproxy.log.'+str(log_file_number)+'.gz .')

def input_log_numbers():
	start_log_file = int(input("\nEnter the starting log file number : "))
	end_log_file = int(input("Enter the ending log file number : "))
	return[start_log_file, end_log_file]



haproxys = ['APP_18.194.19.171', 'PMS_35.158.197.58', 'WWW_35.156.87.211']


for haproxy in haproxys:
	print("**************************************************************************************\n")
	print('\nHAProxy : '+haproxy+'\n')
	log_file_numbers = input_log_numbers()
	download_zip_logs(haproxy[4:], log_file_numbers[0], log_file_numbers[1])
	destination_log_directory = '/home/tech/haproxy_log_analysis/'+haproxy+'/logs'

	os.system('mv /home/tech/*.gz '+destination_log_directory)
	print('All zip files are moved to the '+ haproxy + ' directory')

	os.chdir(destination_log_directory)
	os.system('gunzip *.gz')
	os.chdir('/home/tech')
	print('All zip files are extracted to the '+ haproxy + ' directory')
	print("\n**************************************************************************************\n")