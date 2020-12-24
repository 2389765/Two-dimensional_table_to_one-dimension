import pandas as pd
import numpy as np
import json5
import re
import xlsxwriter

class table_transform():
    def __init__(self, file_path, D):
        ## 文件配置
        setting_file = pd.read_excel(file_path,sheet_name='文件')  
        ## 编码配置
        self.setting_locid = pd.read_excel(file_path,sheet_name='编码', dtype={'编码':str}, index_col='索引列')
        self.setting_locid.index = self.setting_locid.index.str.replace(' ','')
        ## 文件中表的配置
        self.setting_sheet = pd.read_excel(file_path,sheet_name='表', dtype={'表名':str,})
        ## 表中列的配置
        self.setting_col = pd.read_excel(file_path,sheet_name='列', dtype={'比较行代码':str,'地区':str})
        
        # 索引对应的ID编号
        self.ID = self.setting_locid.to_dict()['编码']
        # 待处理文件位置
        self.file_path = setting_file.loc[0,'处理文件路径']  
        # 处理结果输出文件夹
        self.output_path = setting_file.loc[0,'输出文件夹绝对路径'] 
        
        
        self.res_pd = pd.DataFrame
        self.D = D # 保留的小数位数
        
    def sheet_to_process(self,sheet_names=[]):
        sheets = []
        if len(sheet_names)==0:
            for index,sheet in self.setting_sheet.iterrows():
                sheets.append(sheet)
        else:
            for sheet_name in sheet_names:
                try:
                    sheet = self.setting_sheet.loc[self.setting_sheet[''] == sheet_name,:]
                    sheets.append(sheet)
                except:
                    pass
        
        for sheet in sheets:
            self.set_sheet(sheet['表名'],
                           drop_row = sheet['标题前行数'], 
                           head_row = sheet['标题行数'], 
                           index_col = sheet['省份所在列'])
            
            if not self.setting_col[self.setting_col['所在表'] == sheet['表名']].empty:
                for index,row in self.setting_col[self.setting_col['所在表'] == sheet['表名']].iterrows(): self.process_sheet(row)
            self.write_new_excel('Edited-'+str(sheet['表名']))
    
    def set_sheet(self, sheet_name, drop_row, head_row, index_col):
        """
        将读入的excel中待处理的表读入，索引列改为ID，缺失值用0填补
        """
        header = [_ for _ in range(head_row)]
        index_col -= 1
        self.index_col = index_col
        
        # 读入表
        
        self.df = pd.read_excel(io=self.file_path,
              sheet_name=sheet_name, 
              skiprows=drop_row,header=header)
        
        # 将地区改为对应的编码
        self.df.iloc[:,index_col] = self.df.iloc[:,index_col].str.replace(' ','')
        self.df = self.df[self.df.iloc[:,index_col].map(lambda x: x in self.ID)]
        self.df.iloc[:,index_col] = self.df.iloc[:,index_col].map(lambda x: self.ID[x])
        
        # 将空值设为0
        self.df.fillna(0,inplace=True)
        
        self.res_pd = pd.DataFrame
        return   
    
    def process_sheet(self,loc):
        """
        param loc:配置文件-列这个表中的一行，描述这一列的数据的处理方式
        """
        self.get_sheet(self.get_value(loc),loc)
        
    def get_value(self,loc):
        loc_ind = loc['列序号']-1  # 待处理的列
        
        # 处理的值
        value = self.df.iloc[:,loc_ind]
        dot_index = value[value=='…'].index
        value[dot_index] = np.nan
        corss_index = value[value=='-'].index
        value[corss_index] = np.nan
        if loc['计算方式'] != 0 :
            # 当作为减数/除数的数不存在时，则所有的返回结果都应该是不存在
            # 与不应存在的数做计算，结果是不应存在
            if self.df[self.df.iloc[:,self.index_col] == loc['比较行代码']].index in dot_index:
                value[value.index] = '…'
                value[dot_index] = '-'
                return value
            elif self.df[self.df.iloc[:,self.index_col] == loc['比较行代码']].index in corss_index:
                value[value.index] = '-'
                return value
            value = self.calculate(self.df.iloc[:,[loc_ind,self.index_col]], loc['计算方式'], loc['比较行代码'])
        
        # 保存为D位的小数
        value = value.astype(np.float64)
        value = value.round(decimals=self.D)
        value[dot_index] = '…'
        value[corss_index] = '-'
        return value
        
    def get_sheet(self,value,loc):
        """
        生成一维表
        """
        # 生成一维表
        pd_dict = {"列表":loc["列表"],"唯一名称":" ","指标构成1":loc["指标构成1"],"指标构成2":loc["指标构成2"],
                   "指标构成3":loc["指标构成3"],"指标构成4":loc["指标构成4"],"地区":loc["地区"],
                   "频率":loc["频率"],"单位":loc["单位"],"数据来源":loc["数据来源"],"年":loc["年"],"季":loc["季"],
                   "月":loc["月"],"数值":value}
        
        # 将对应的列的内容改为索引列的编码
        for key in pd_dict:
            if pd_dict[key] == '处理':
                pd_dict[key] = list(self.df.iloc[:,self.index_col]) # 替换为对应的编码
                
                # 对于做比较的情况，基准线的数据删除
                if loc['计算方式'] == 2 or loc['计算方式'] == 3:
                    drop_index = pd_dict[key].index(loc['比较行代码'])
                    pd_dict[key].pop(drop_index)
                    pd_dict["数值"].pop(drop_index)                
                break
        

            
        res_pd = pd.DataFrame(pd_dict)
        res_pd.fillna(method='pad',inplace=True)
        res_pd.reset_index(drop=True,inplace=True)
        # 将空值设为空格
        res_pd.fillna(' ',inplace=True)
        # 生成唯一名称
        res_pd.loc[:,'唯一名称'] = res_pd.loc[:,'地区']+res_pd.loc[:,'指标构成3']+\
                                  res_pd.loc[:,'指标构成1']+res_pd.loc[:,'指标构成2']+res_pd.loc[:,'指标构成4']

        res_pd.loc[:,'唯一名称'].replace('\s+','',regex=True,inplace=True)
        
        
        # 同一个表的多个列对应的表连接起来一同输出
        if self.res_pd.empty:
            self.res_pd=res_pd
        else:
            self.res_pd = pd.concat([self.res_pd,res_pd],ignore_index=True)
    
    def write_new_excel(self,name):
        """
        输出表处理后的结果
        """
        self.res_pd.to_excel(self.output_path+'\\'+name+'.xlsx',
                             engine='xlsxwriter',index=False)   
    
    def calculate(self,series, method, compare):
        
        # 万转为亿
        if method == 1:
            return series.iloc[:,0]/10000
        # 与全国做除法
        elif method == 2:
            national_number = series[(series.iloc[:,1] == compare)].iloc[0,0]
            return series.iloc[:,0]/national_number
        # 与全国做减法
        
        elif method == 3:
            national_number = series[(series.iloc[:,1] == compare)].iloc[0,0]
            return series.iloc[:,0]-national_number
        else:
            return series.iloc[:,0]
        

if __name__ == '__main__':
    file_path = input("excel配置文件地址：")
    D = int(input("保留的小数位数："))
    # file_path = 'C:\\Users\\iceberg\\Desktop\\河北省处理配置文件.xlsx'
    TT = table_transform(file_path,D)
    TT.sheet_to_process()
    print("操作完成!!!!!!!!!!!!!!!!")
