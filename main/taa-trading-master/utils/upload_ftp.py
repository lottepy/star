import pysftp

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

# with pysftp.Connection(
# 	host='ftp.dymonasia.com',
# 	username='magnumwm',
# 	password='D7yu#3iQr!',
# 	cnopts=cnopts
# ) as sftp:
# 	filename = 'sample_weight.csv'
# 	sftp.put(filename)
# 	sftp.close()

sftp = pysftp.Connection(
	host='ftp.dymonasia.com',
	username='magnumwm',
	password='D7yu#3iQr!',
	cnopts=cnopts
)
dir = sftp.listdir()
filename = 'Aqumon2Dymon_TargetRatio_20191121.csv'
sftp.put(filename)
sftp.close()


