# -*- coding:utf-8 -*-
# 应被外部调用 不直接执行此文件

from .base_strategy import BaseStrategy


class TargetPositionStrategy(BaseStrategy):

    def __init__(self, data_dir: str, target_prefix='', target_suffix='', is_local=False):
        super().__init__(data_dir, target_prefix, target_suffix, is_local)

    def go(self):
        self.target_holdings.index.name = 'iuid'
        self.target_holdings.columns = ['position']
        self.target_holdings.sort_index().to_csv('target_position.csv')
