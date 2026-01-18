import pandas as pd
import openpyxl
from typing import Dict, List, Union, Tuple, Optional
import os
import logging
import re
import datetime
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataReader:
    """
    负责从Excel文件读取数据的类
    """
    
    def __init__(self, file_path: str):
        """
        初始化DataReader
        
        Args:
            file_path: Excel文件的路径
        """
        self.file_path = file_path
        self._validate_file()
        
    def _validate_file(self) -> None:
        """验证文件是否存在且是有效的Excel文件"""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"文件不存在: {self.file_path}")
        
        if not self.file_path.endswith(('.xlsx', '.xls')):
            raise ValueError(f"文件不是有效的Excel文件: {self.file_path}")
        
        logger.info(f"已验证文件: {self.file_path}")
    
    def get_sheet_names(self) -> List[str]:
        """
        获取Excel文件中所有工作表的名称
        
        Returns:
            工作表名称列表
        """
        try:
            wb = openpyxl.load_workbook(self.file_path, read_only=True)
            sheet_names = wb.sheetnames
            wb.close()
            return sheet_names
        except Exception as e:
            logger.error(f"获取工作表名称时出错: {str(e)}")
            raise
    
    def read_sheet_data(self, sheet_name: str) -> pd.DataFrame:
        """
        读取指定工作表的所有数据
        
        Args:
            sheet_name: 工作表名称
            
        Returns:
            包含工作表数据的DataFrame
        """
        try:
            # 读取Excel文件时不使用第一行作为列名
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=None)
            logger.info(f"已读取工作表 '{sheet_name}' 的数据，共 {len(df)} 行")
            return df
        except Exception as e:
            logger.error(f"读取工作表 '{sheet_name}' 数据时出错: {str(e)}")
            raise
    
    def read_indicator_data(self, sheet_name: str, indicator_code: str) -> Dict:
        """
        读取指定工作表中特定指标代码的数据
        
        Args:
            sheet_name: 工作表名称
            indicator_code: 指标代码
            
        Returns:
            字典，包含元数据和时间序列数据
        """
        try:
            # 读取整个工作表
            df = self.read_sheet_data(sheet_name)
            
            # 前10行为元数据，第11行开始为数据  
            metadata_rows = df.iloc[:10]  # 修改回前10行为元数据
            data_rows = df.iloc[10:]      # 修改回从第11行开始为数据
            
            # 重置数据行的索引
            data_rows = data_rows.reset_index(drop=True)
            
            # 提取元数据
            metadata = {}
            for i, row in metadata_rows.iterrows():
                # 第一列是元数据名称，其他列是对应的值
                metadata_name = row.iloc[0]
                metadata_values = row.iloc[1:].tolist()
                metadata[metadata_name] = metadata_values
            
            # 查找指标代码行
            indicator_code_row_index = None
            for i, row in metadata_rows.iterrows():
                if row.iloc[0] == '指标代码':
                    indicator_code_row_index = i
                    break
            
            if indicator_code_row_index is None:
                raise ValueError(f"在工作表 '{sheet_name}' 中未找到指标代码行")
            
            # 获取指标代码行
            indicator_code_row = metadata_rows.iloc[indicator_code_row_index]
            
            # 查找匹配的指标代码索引
            code_index = None
            for i, code in enumerate(indicator_code_row.iloc[1:], 1):  # 从1开始，跳过第一列
                if code == indicator_code:
                    code_index = i - 1  # 调整为从0开始的索引
                    break
            
            if code_index is None:
                logger.warning(f"在工作表 '{sheet_name}' 中未找到指标代码 '{indicator_code}'")
                return {"metadata": {}, "data": pd.DataFrame()}
            
            # 提取该指标的元数据
            indicator_metadata = {}
            for key, row in zip(metadata.keys(), metadata_rows.iterrows()):
                _, row_data = row
                if code_index + 1 < len(row_data):
                    indicator_metadata[key] = row_data.iloc[code_index + 1]
            
            # # 打印指标的元数据
            # print(f"\n指标 '{indicator_code}' 的元数据:")
            # for key, value in indicator_metadata.items():
            #     print(f"  {key}: {value}")
            
            # 找到指标列的索引（在数据中的位置）
            indicator_col_index = code_index + 1
            
            # 打印指标位置信息
            # print(f"指标 '{indicator_code}' 在数据中的位置: 第 {indicator_col_index + 1} 列")
            # logger.info(f"指标 '{indicator_code}' 在数据中的位置: 第 {indicator_col_index + 1} 列")
            
            # 查找该指标左侧的时间列
            # 从指标代码行向左查找，找到第一个值为"指标代码"的列
            time_col_index = 0  # 默认使用第一列
            
            # print(f"指标代码行: {indicator_code_row.tolist()}")
            
            # 从指标列向左查找，直到找到值为"指标代码"的列
            for i in range(indicator_col_index, 0, -1):
                # print(f"检查列 {i}: 值 = {indicator_code_row.iloc[i]}")
                # 检查是否是"指标代码"
                if indicator_code_row.iloc[i] == '指标代码':
                    # 找到了左侧的"指标代码"列，该列就是时间列
                    time_col_index = i
                    print(f"找到左侧'指标代码'列: 第 {i + 1} 列，将其作为时间列")
                    break
            
            logger.info(f"指标 '{indicator_code}' 的时间列索引: {time_col_index}")
            
            # 提取该指标的数据列和对应的时间列
            if indicator_col_index < len(data_rows.columns):
                indicator_data = data_rows.iloc[:, [time_col_index, indicator_col_index]]
                
                # 重命名列
                indicator_name = indicator_metadata.get('指标全称', f'指标_{indicator_code}')
                indicator_data.columns = ['日期', indicator_name]
                
                # 尝试将日期列转换为日期格式
                try:
                    # 在 DataReader 类的 read_indicator_data 方法中，修改日期转换的代码
                    # 将类似这样的代码：
                    indicator_data['日期'] = pd.to_datetime(indicator_data['日期'])
                    
                    # 修改为：
                    indicator_data = indicator_data.copy()  # 创建明确的副本
                    indicator_data['日期'] = pd.to_datetime(indicator_data['日期'])
                except:
                    logger.warning(f"无法将指标 '{indicator_code}' 的时间列转换为日期格式")
                
                logger.info(f"已读取指标 '{indicator_code}' 的数据，共 {len(indicator_data)} 行")
                
                # 返回包含元数据和数据的字典
                return {
                    "metadata": indicator_metadata,
                    "data": indicator_data
                }
            else:
                logger.warning(f"指标 '{indicator_code}' 的数据列不存在")
                return {
                    "metadata": indicator_metadata,
                    "data": pd.DataFrame()
                }
            
        except Exception as e:
            logger.error(f"读取指标 '{indicator_code}' 数据时出错: {str(e)}")
            raise

    def _is_date(self, value):
        """
        判断一个值是否可能是日期
        
        Args:
            value: 要检查的值
            
        Returns:
            布尔值，表示是否可能是日期
        """
        # 打印调试信息
        print(f"检查值是否为日期: {value}, 类型: {type(value)}")
        
        # 如果已经是日期类型
        if isinstance(value, (pd.Timestamp, datetime.datetime, datetime.date)):
            print(f"值 {value} 是日期类型")
            return True
        
        # 如果是字符串，尝试转换为日期
        if isinstance(value, str):
            try:
                pd.to_datetime(value)
                print(f"字符串 '{value}' 可以转换为日期")
                return True
            except:
                pass
        
        # 如果是数字类型，可能是Excel日期序列号
        if isinstance(value, (int, float)) and not pd.isna(value):  # 添加非NaN检查
            try:
                # 尝试将Excel序列号转换为日期
                excel_epoch = datetime.datetime(1899, 12, 30)  # Excel的纪元日期
                date_value = excel_epoch + datetime.timedelta(days=value)
                # 检查是否是合理的日期
                current_year = datetime.datetime.now().year
                if 1900 <= date_value.year <= current_year + 10:
                    print(f"数值 {value} 可以转换为日期: {date_value}")
                    return True
            except:
                pass
        
        # 特殊处理：检查是否是pandas Timestamp对象的字符串表示
        if isinstance(value, str) and ('timestamp' in value.lower() or 
                                      any(x in value.lower() for x in ['年', '月', '日', '-', '/', '.'])):
            try:
                pd.to_datetime(value)
                print(f"特殊字符串 '{value}' 可以转换为日期")
                return True
            except:
                # 检查常见的日期格式模式
                date_patterns = [
                    r'\d{4}-\d{1,2}-\d{1,2}',  # YYYY-MM-DD
                    r'\d{1,2}/\d{1,2}/\d{4}',  # MM/DD/YYYY
                    r'\d{4}/\d{1,2}/\d{1,2}',  # YYYY/MM/DD
                    r'\d{4}\.\d{1,2}\.\d{1,2}', # YYYY.MM.DD
                    r'\d{4}年\d{1,2}月\d{1,2}日', # YYYY年MM月DD日
                    r'\d{4}-\d{1,2}',  # YYYY-MM
                    r'\d{4}/\d{1,2}',  # YYYY/MM
                    r'\d{4}\.\d{1,2}',  # YYYY.MM
                    r'\d{4}年\d{1,2}月'  # YYYY年MM月
                ]
                
                for pattern in date_patterns:
                    if re.match(pattern, value):
                        print(f"字符串 '{value}' 匹配日期模式")
                        return True
        
        # 直接尝试转换，不管类型
        try:
            date_obj = pd.to_datetime(value)
            if not pd.isna(date_obj):
                print(f"值 {value} 可以直接转换为日期: {date_obj}")
                return True
        except:
            pass
        
        print(f"值 {value} 不是日期")
        return False

# 使用示例
if __name__ == "__main__":
    # 示例用法
    reader = DataReader("D:\【交付审核】\_导出验收\Excel数据验收\数据导出页.xlsx")
    
    # 获取所有工作表名称
    sheet_names = reader.get_sheet_names()
    print(f"工作表列表: {sheet_names}")
    
    # 读取特定指标的数据
    indicator_data = reader.read_indicator_data("1.1电力", "AA06")
    print(f"指标 AA001 数据预览:\n{indicator_data['data'].head()}")
    
 
    