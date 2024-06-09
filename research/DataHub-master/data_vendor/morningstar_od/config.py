from enum import Enum

MASTER = "/Users/yujiaqi/PycharmProjects/MorningstarAPI"
BASE_URL = 'http://edw.morningstar.com/'
FUNDAMENTAL_BASE_URL = 'DataOutput.aspx'
HISTORICAL_BASE_URL = 'HistoryData/HistoryData.aspx?'
CONTENT_TYPE = 'text/xml; charset=utf-8'
PACKAGE = 'EDW'
CLIENT_ID = 'magnumhk'


class XPATH(Enum):
    INCEPTION_DATE = '/FundShareClass/Operation/ShareClassBasics/InceptionDate/'
    MANAGER_LIST = '/FundShareClass/Fund/FundManagement/ManagerList/'
    INVESTMENT_OBJ = '/FundShareClass/Fund/MultilingualVariation/LanguageVariation/RegionVariation/FundNarratives/InvestmentCriteria/StrategySummary/'
    GIFS_CODE = '/FundShareClass/SP_CodeAndValue/GIFSCode/'
    MPT_BENCHMARK = '/FundShareClass/Fund/FundBasics/MPTRiskFreeRateBenchmark/'
    PRIMARY_BENCHMARK = '/FundShareClass/Fund/FundBasics/PrimaryBenchmark/'
    SECONDARY_BENCHMARK = '/FundShareClass/Fund/FundBasics/SecondaryBenchmark/'
    NET_EXPENSERATIO = '/FundShareClass/Operation/AnnualReport/FeeAndExpense/NetExpenseRatio/'
    MANAGEMENT_FEE = '/FundShareClass/Operation/Prospectus/ManagementFee/Value/'
    TURNOVER_RATIO = '/FundShareClass/Fund/AnnualReport/Financials/TurnoverRatio/'
    FUND_OWNER = '/FundShareClass/Fund/FundManagement/ProviderCompany/Company/CompanyOperation/CompanyIdentifiers/CompanyIdentifiers/'

    CREDIT_QUALITY = '/FundShareClass/Fund/FundBasics/MPTRiskFreeRateBenchmark/HoldingDetail/CreditQuality/'
    DURATION = '/FundShareClass/Fund/FundBasics/MPTRiskFreeRateBenchmark/HoldingDetail/Duration/'
    YIELD_TO_MATURITY = '/FundShareClass/Fund/FundBasics/MPTRiskFreeRateBenchmark/HoldingDetail/YieldtoMaturity/'
    MATURITY_DATE = '/FundShareClass/Fund/FundBasics/SecondaryBenchmark/HoldingDetail/MaturityDate/'

    MANAGER_GIVEN_NAME = '/PersonalInformation/GivenName/'
    MANAGER_MIDDLE_NAME = '/PersonalInformation/MiddleName/'
    MANAGER_FAMILY_NAME = '/PersonalInformation/FamilyName/'
    MANAGER_CAREER_START_YEAR = '/PersonalInformation/CareerStartYear/'
    MANAGER_COLLEGE = '/PersonalInformation/CollegeEducation/CollegeEducationDetail/School/'
    MANAGER_BIOGRAPHY = '/PersonalInformation/Biography/ProfessionalBiography/'