import oss2
import os,time,sys

####
acc_id = sys.argv[1]
acc_sec = sys.argv[2]
oss_storage_path = sys.argv[3]
##
bucket_name = 'novo-data-nj'
##
ENDPOINT = 'http://oss-cn-hangzhou.aliyuncs.com'

cur_date = time.strftime('%Y_%m',time.localtime(time.time()))
workdir = sys.argv[4]

auth = oss2.Auth(acc_id, acc_sec)
bucket = oss2.Bucket(auth, ENDPOINT, bucket_name)

ossutil = '/home/src/gopath/bin/ossutil'

##get\create workdir##
def get_work_dir():
	if not os.path.exists(workdir):
		#print('workdir doesn\'t exist,creating')
		try:
			os.makedirs(workdir)
			#print('success')
		except Exception as e:
			#print('failed:')
			raise e
	batches = os.listdir(workdir)
	nums = [0]
	#print(type(nums),nums)
	for x in batches:
		nums.append(int(x[-1])) 
	if not os.path.isfile(workdir+cur_date.split('_')[-1]+'_'+'batch'+str(max(nums))+'/.finished'):
		return workdir+cur_date.split('_')[-1]+'_'+'batch'+str(max(nums))+'/'
	else:
		return workdir+cur_date.split('_')[-1]+'_'+'batch'+str(max(nums)+1)+'/'

##get .gz s##
def get_file_list(download_dir):
	os.system('{} config -i {} -k {} -e {}'.format(ossutil,acc_id,acc_sec,ENDPOINT))
	file_detail_list = os.popen('{} ls {}'.format(ossutil,oss_storage_path))
	f = file_detail_list.read()
	print(f)
	file_list = []
	for x in f.split('\n'):
		if oss_storage_path in x:
			file = x.split(' ')[-1]
			if(file.endswith('.gz')):
				
				if not os.path.exists(download_dir+file.split('/')[-1].split('_')[0]+'/'):
					os.makedirs(download_dir+file.split('/')[-1].split('_')[0]+'/')
				file_list.append(file)
	print(file_list)
	return file_list

def check_sum():
	pass

def main():
	download_dir = get_work_dir()
	#print('will download to :',download_dir)
	for file in get_file_list(download_dir):
		pass
		#print(file)
		#bucket.get_object_to_file(file.split(bucket_name+'/')[-1], download_dir+file.split('/')[-1].split('_')[0]+'/'+file.split('/')[-1])
	#os.mknod(download_dir+'.finished')
	#print('download finished')

if __name__ == '__main__':
	main()