import pandas as pd

from config import MASTER
from config import BASE_URL
from config import CONTENT_TYPE
from config import PACKAGE
from config import CLIENT_ID

class BaseAPI(object):
	"""Base class for Morningstar API

	The code defines all available API methods for calling data of mutual funds.
	"""

	BASE_URL = BASE_URL
	CONTENT_TYPE = CONTENT_TYPE
	PACKAGE = PACKAGE
	CLIENT_ID = CLIENT_ID

	def __init__(self):
		pass

	def get_inception_date(self, secid):
		raise NotImplementedError("get_inception_date not implement yet.")

	def get_manager_list(self, secid):
		raise NotImplementedError("get_manager_list not implement yet.")

	def get_investment_obj(self, secid):
		raise NotImplementedError("get_investment_obj not implement yet.")

	def get_gifs_code(self, secid):
		raise NotImplementedError("get_gifs_code not implement yet.")

	def get_mpt_benchmark(self, secid):
		raise NotImplementedError("get_mpt_benchmark not implement yet.")

	def get_primary_benchmark(self, secid):
		raise NotImplementedError("get_primary_benchmark not implement yet.")

	def get_secondary_benchmark(self, secid):
		raise NotImplementedError("get_secondary_benchmark not implement yet.")

	def get_portfolio_holding(self, masterportfolioid):
		raise NotImplementedError("get_portfolio_holding not implement yet.")

	def get_asset_allocation(self, masterportfolioid):
		raise NotImplementedError("get_asset_allocation not implement yet.")

	def get_region_allocation(self, masterportfolioid):
		raise NotImplementedError("get_region_allocation not implement yet.")

	def get_sector_allocation(self, masterportfolioid):
		raise NotImplementedError("get_sector_allocation not implement yet.")

	def get_credit_allocation(self, masterportfolioid):
		raise NotImplementedError("get_credit_allocation not implement yet.")

	def get_maturity_allocation(self, masterportfolioid):
		raise NotImplementedError("get_maturity_allocation not implement yet.")

	def get_currency_allocation(self, masterportfolioid):
		raise NotImplementedError("get_currency_allocation not implement yet.")

	def get_aum(self, performanceid):
		raise NotImplementedError("get_aum not implement yet.")

	def get_expense_ratio(self, secid):
		raise NotImplementedError("get_expense_ratio not implement yet.")

	def get_management_fee(self, secid):
		raise NotImplementedError("get_management_fee not implement yet.")

	def get_turnover_rate(self, secid):
		raise NotImplementedError("get_turnover_rate not implement yet.")

	def get_nav(self, performanceid):
		raise NotImplementedError("get_nav not implement yet.")

	def get_adjusted_nav(self, performanceid):
		raise NotImplementedError("get_adjusted_nav not implement yet.")

	def get_historical_fund_owner(self, secid):
		raise NotImplementedError("get_historical_fund_owner not implement yet.")

	def get_master_portid(self, secid):
		raise NotImplementedError("get_master_portid not implement yet.")

	def get_portfolio_info(self, masterportfolioid):
		raise NotImplementedError("get_portfolio_info not implement yet.")

	def get_fundamental_info(self, secid):
		raise NotImplementedError("get_basic_info not implement yet.")

	def get_daily_data(self, secid):
		raise NotImplementedError("get_daily_data not implement yet.")


	@staticmethod
	def get_tree_element(tree, xpath):
		"""
		Get target element in element tree
		:param tree: elementtree
		:param xpath: str
		"""
		elements = xpath.split("/")
		if (tree is None) or (len(elements)<=2):
			return tree
		else:
			ele = elements[1]
			try:
				new_tree = list(tree.getiterator(ele))[0]
			except:
				new_tree = None
			del elements[1]
			new_xpath = "/".join(elements)
			return BaseAPI.get_tree_element(new_tree, new_xpath)


	def get_tree_element_old(tree, xpath):
		"""
		Get target element in element tree
		:param tree: elementtree
		:param xpath: str
		"""
		elements = xpath.split("/")
		if len(elements) <= 2:
			return tree.text
		else:
			ele = elements[1]
			new_tree = list(tree.getiterator(ele))[0]
			del elements[1]
			new_xpath = "/".join(elements)
			return BaseAPI.get_tree_element(new_tree, new_xpath)


	def _safe_return(self, tree):
		"""
		Try to return the text result. If fail, return None
		:param tree:
		:return: str, or list, or None
		"""
		try:
			result = list(tree.itertext())
			if len(result)==1:
				return result[0]
			return result
		except:
			return None

	def _get_xpath_text(self, xpath, secid=None):
		"""

		:param xpath:
		:param secid:
		:return:
		"""
		content = self.content
		if secid is not None:
			content = self.reset_secid(secid=secid)
		tree = self.get_tree_element(tree=content,
									 xpath=xpath
									 )
		return self._safe_return(tree)


class MasterID(object):
	"""Master ID dictionary

	This code is used for mapping different IDs.
	"""
	ID = pd.read_csv("{}/{}".format(MASTER, 'cn_fund.csv'))

	def __init__(self):
		self.ID.loc[:, 'CSDCCCode'] = self.ID.loc[:, 'CSDCCCode'].apply(self._convert_csdcc_to_six_digits)

	def _convert_csdcc_to_six_digits(self, csdcc):
		try:
			return str(int(csdcc)).zfill(6)
		except:
			return csdcc

	def from_isin_to_secid(self, isin):
		secid = self.ID[self.ID['ISIN']==isin]['SecID'].values[0]
		return str(secid)

	def from_isin_to_performanceid(self, isin):
		performanceid = self.ID[self.ID['ISIN'] == isin]['PerformanceID'].values[0]
		return str(performanceid)

	def from_isin_to_masterportfolioid(self, isin):
		masterportfolioid = self.ID[self.ID['ISIN'] == isin]['MasterPortfolioId'].values[0]
		if isinstance(masterportfolioid, float):
			return str(int(masterportfolioid))
		return str(masterportfolioid)

	def from_secid_to_isin(self, secid):
		isin = self.ID[self.ID['SecID'] == secid]['ISIN'].values[0]
		return str(isin)

	def from_secid_to_performanceid(self, secid):
		performanceid = self.ID[self.ID['SecID']==secid]['PerformanceID'].values[0]
		return str(performanceid)

	def from_secid_to_masterportfolioid(self, secid):
		masterportfolioid = self.ID[self.ID['SecID'] == secid]['MasterPortfolioId'].values[0]
		if isinstance(masterportfolioid, float):
			return str(int(masterportfolioid))
		return str(masterportfolioid)

	def from_csdcc_to_isin(self, csdcc):
		isin = self.ID[self.ID['CSDCCCode'] == csdcc]['ISIN'].values[0]
		return str(isin)

	def from_csdcc_to_secid(self, csdcc):
		secid = self.ID[self.ID['CSDCCCode'] == csdcc]['SecID'].values[0]
		return str(secid)

	def from_csdcc_to_performanceid(self, csdcc):
		performanceid = self.ID[self.ID['CSDCCCode'] == csdcc]['PerformanceID'].values[0]
		return str(performanceid)

	def from_csdcc_to_masterportfolioid(self, csdcc):
		masterportfolioid = self.ID[self.ID['CSDCCCode'] == csdcc]['MasterPortfolioId'].values[0]
		if isinstance(masterportfolioid, float):
			return str(int(masterportfolioid))
		return str(masterportfolioid)



if __name__=="__main__":
	# print("Test Base API")
	# api = BaseAPI()
	# api.get_inception_date("test_id")

	print("Test Master ID")
	mid = MasterID()
	# secid = mid.from_isin_to_secid(isin='LU0106261372')
	# perfid = mid.from_isin_to_performanceid(isin='LU0106261372')
	# masterportfolioid = mid.from_isin_to_masterportfolioid(isin='LU0106261372')
	# print(secid, perfid, masterportfolioid)
	#
	# isin = mid.from_secid_to_isin(secid=secid)
	# perfid = mid.from_secid_to_performanceid(secid=secid)
	# masterportfolioid = mid.from_secid_to_masterportfolioid(secid=secid)
	# print(isin, perfid, masterportfolioid)

	isin = mid.from_csdcc_to_isin(csdcc="110023")
	secid = mid.from_csdcc_to_secid(csdcc="000043")
	perfid = mid.from_csdcc_to_performanceid(csdcc="000043")
	masterportfolioid = mid.from_csdcc_to_masterportfolioid(csdcc="000043")

	print()

