# -*- coding: utf-8 -*-
from portfolio_analysis.portfolio import Portfolio
from portfolio_analysis.portfolio_analytics import PortfolioAnalytics


pos_path = '../data/bocam_position.csv'
price_path = '../data/bocam_price.csv'
benchmark_pos_path = '../data/bocam_benchmark_position.csv'
benchmark_price_path = '../data/bocam_benchmark_price.csv'
mapping_path = '../data/bocam_underlying_mapping.csv'
output_excel_path = '../result/bocam_report.xlsx'

bocam_portfolio = Portfolio.from_csv(pos_path, 'BOCAM')
bocam_portfolio.fetch_price(method='csv', path=price_path)
# bocam_portfolio.simple_analysis()

bocam_benchmark = Portfolio.from_csv(benchmark_pos_path, 'BOCAM-Benchmark')
bocam_benchmark.fetch_price(method='csv', path=benchmark_price_path)
# bocam_benchmark.simple_analysis()

bocam_pa = PortfolioAnalytics(bocam_portfolio, bocam_benchmark)
bocam_pa.link_underlying(mapping_path)
bocam_pa.return_attribution(method='Parilux')
print bocam_pa.describe().to_string()

bocam_pa.to_excel(output_excel_path)