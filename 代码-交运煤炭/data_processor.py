import pandas as pd
import numpy as np
from typing import Dict, List, Union, Optional, Tuple
import logging
from datetime import datetime, timedelta
from data_reader import DataReader

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataProcessor:
    """
    负责处理数据和执行衍生计算的类
    """
    
    def __init__(self, data_reader: DataReader):
        """
        初始化DataProcessor
        
        Args:
            data_reader: DataReader实例，用于读取原始数据
        """
        self.data_reader = data_reader
    
    def filter_by_date_range(self, indicator_data: Dict, start_date: str = None, end_date: str = None) -> Dict:
        """
        筛选指定时间范围内的指标数据
        
        Args:
            indicator_data: 指标数据字典，包含metadata和data
            start_date: 开始日期，格式为'YYYY-MM-DD'，如果为None则不限制开始日期
            end_date: 结束日期，格式为'YYYY-MM-DD'，如果为None则不限制结束日期
            
        Returns:
            筛选后的指标数据字典
        """
        try:
            # 复制原始数据，避免修改原始数据
            result = {
                "metadata": indicator_data["metadata"].copy(),
                "data": indicator_data["data"].copy()
            }
            
            # 确保日期列是datetime类型
            if not pd.api.types.is_datetime64_any_dtype(result["data"]["日期"]):
                result["data"]["日期"] = pd.to_datetime(result["data"]["日期"], errors='coerce')
            
            # 筛选日期范围
            mask = pd.Series(True, index=result["data"].index)
            
            if start_date:
                start_date = pd.to_datetime(start_date)
                mask = mask & (result["data"]["日期"] >= start_date)
                
            if end_date:
                end_date = pd.to_datetime(end_date)
                mask = mask & (result["data"]["日期"] <= end_date)
            
            result["data"] = result["data"][mask].reset_index(drop=True)
            
            logger.info(f"已筛选时间范围: {start_date} 至 {end_date}, 剩余数据行数: {len(result['data'])}")
            
            return result
        
        except Exception as e:
            logger.error(f"筛选时间范围时出错: {str(e)}")
            raise
    
    def sort_by_date(self, indicator_data: Dict, ascending: bool = True) -> Dict:
        """
        按日期对指标数据进行排序
        
        Args:
            indicator_data: 指标数据字典，包含metadata和data
            ascending: 是否升序排列，True为升序，False为降序
            
        Returns:
            排序后的指标数据字典
        """
        try:
            # 复制原始数据，避免修改原始数据
            result = {
                "metadata": indicator_data["metadata"].copy(),
                "data": indicator_data["data"].copy()
            }
            
            # 确保日期列是datetime类型
            if not pd.api.types.is_datetime64_any_dtype(result["data"]["日期"]):
                result["data"]["日期"] = pd.to_datetime(result["data"]["日期"], errors='coerce')
            
            # 按日期排序
            result["data"] = result["data"].sort_values(by="日期", ascending=ascending).reset_index(drop=True)
            
            order_type = "升序" if ascending else "降序"
            logger.info(f"已按日期{order_type}排列数据")
            
            return result
        
        except Exception as e:
            logger.error(f"按日期排序时出错: {str(e)}")
            raise
    
    def create_month_year_pivot(self, indicator_data: Dict, value_column: str = None) -> pd.DataFrame:
        """
        创建以月份为横坐标、年份为纵坐标的交叉表
        
        Args:
            indicator_data: 指标数据字典，包含metadata和data
            value_column: 要展示的值列名，如果为None则使用第二列（通常是指标值）
            
        Returns:
            交叉表DataFrame
        """
        try:
            # 获取数据
            data = indicator_data["data"].copy()
            
            # 确保日期列是datetime类型
            if not pd.api.types.is_datetime64_any_dtype(data["日期"]):
                data["日期"] = pd.to_datetime(data["日期"], errors='coerce')
            
            # 提取年份和月份
            data["年份"] = data["日期"].dt.year
            data["月份"] = data["日期"].dt.month
            
            # 确定值列
            if value_column is None:
                # 使用第二列作为值列
                value_column = data.columns[1]
            
            # 创建交叉表
            pivot_table = pd.pivot_table(
                data,
                values=value_column,
                index="年份",
                columns="月份",
                aggfunc="mean"  # 如果同一年月有多个值，取平均值
            )
            
            # 重命名列，使用月份名称
            month_names = {
                1: "一月", 2: "二月", 3: "三月", 4: "四月",
                5: "五月", 6: "六月", 7: "七月", 8: "八月",
                9: "九月", 10: "十月", 11: "十一月", 12: "十二月"
            }
            pivot_table = pivot_table.rename(columns=month_names)
            
            logger.info(f"已创建月份-年份交叉表，形状: {pivot_table.shape}")
            
            return pivot_table
        
        except Exception as e:
            logger.error(f"创建月份-年份交叉表时出错: {str(e)}")
            raise
    
    def calculate_year_over_year_change(self, indicator_data: Dict) -> Dict:
        """
        计算同比变化率
        
        Args:
            indicator_data: 指标数据字典，包含metadata和data
            
        Returns:
            包含原始数据和同比变化率的字典
        """
        try:
            # 复制原始数据，避免修改原始数据
            result = {
                "metadata": indicator_data["metadata"].copy(),
                "data": indicator_data["data"].copy()
            }
            
            # 确保日期列是datetime类型
            if not pd.api.types.is_datetime64_any_dtype(result["data"]["日期"]):
                result["data"]["日期"] = pd.to_datetime(result["data"]["日期"], errors='coerce')
            
            # 获取值列名
            value_column = result["data"].columns[1]
            
            # 按日期排序
            result["data"] = result["data"].sort_values(by="日期").reset_index(drop=True)
            
            # 添加年份和月份列
            result["data"]["年份"] = result["data"]["日期"].dt.year
            result["data"]["月份"] = result["data"]["日期"].dt.month
            
            # 创建同比变化率列
            result["data"]["同比变化率"] = np.nan
            
            # 计算同比变化率
            for i, row in result["data"].iterrows():
                # 查找去年同期数据
                last_year = row["年份"] - 1
                same_month = row["月份"]
                
                last_year_data = result["data"][
                    (result["data"]["年份"] == last_year) & 
                    (result["data"]["月份"] == same_month)
                ]
                
                if not last_year_data.empty:
                    current_value = row[value_column]
                    last_year_value = last_year_data.iloc[0][value_column]
                    
                    if last_year_value != 0:
                        yoy_change = (current_value - last_year_value) / last_year_value
                        result["data"].at[i, "同比变化率"] = yoy_change
            
            # 删除辅助列
            result["data"] = result["data"].drop(columns=["年份", "月份"])
            
            logger.info(f"已计算同比变化率")
            
            return result
        
        except Exception as e:
            logger.error(f"计算同比变化率时出错: {str(e)}")
            raise
    
    def merge_indicators(self, indicators: List[Dict], on: str = "日期") -> pd.DataFrame:
        """
        合并多个指标数据到一个DataFrame
        
        Args:
            indicators: 指标数据字典列表
            on: 合并的键，默认为"日期"
            
        Returns:
            合并后的DataFrame
        """
        try:
            if not indicators:
                return pd.DataFrame()
            
            # 初始化结果DataFrame
            result = indicators[0]["data"].copy()
            
            # 合并其他指标
            for i in range(1, len(indicators)):
                # 获取当前指标数据
                current_data = indicators[i]["data"].copy()
                
                # 确保日期列是datetime类型
                if not pd.api.types.is_datetime64_any_dtype(result[on]):
                    result[on] = pd.to_datetime(result[on], errors='coerce')
                
                if not pd.api.types.is_datetime64_any_dtype(current_data[on]):
                    current_data[on] = pd.to_datetime(current_data[on], errors='coerce')
                
                # 合并数据
                result = pd.merge(result, current_data, on=on, how="outer")
            
            # 按日期排序
            result = result.sort_values(by=on).reset_index(drop=True)
            
            logger.info(f"已合并 {len(indicators)} 个指标数据，结果形状: {result.shape}")
            
            return result
        
        except Exception as e:
            logger.error(f"合并指标数据时出错: {str(e)}")
            raise

