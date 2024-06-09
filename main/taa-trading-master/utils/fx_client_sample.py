from fx_client_lib.fx_client import get_oss_data


def main():
	data, time = get_oss_data(['AUDCAD', 'AUDJPY', 'USDIDR'], '2018-09-01', '2019-11-02')
	print(data)
	print(time)
	# print(file_mapping_dict)


if __name__ == '__main__':
	main()
