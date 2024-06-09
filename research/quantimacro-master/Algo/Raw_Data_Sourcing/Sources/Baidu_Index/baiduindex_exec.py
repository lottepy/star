
from selenium import webdriver

from Algo.Raw_Data_Sourcing.Sources.Baidu_Index.baiduindex_funcs import *

keyword = '职位'
driver = init_spider(keyword)

aveIndex(driver)
defaultIndex(driver)

url = 'http://index.baidu.com/'
driver = webdriver.Chrome(executable_path='C:/Program Files (x86)/chromedriver_win32/chromedriver.exe')
driver.get(url)
time.sleep(30)
cookies = driver.get_cookies()
print(cookies)