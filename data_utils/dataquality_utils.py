import pandas as pd
import time
import datetime


class DataQualityUtils:
    
    def __init__(self,
                 data):
        
        self.data = data
        self.qtd_rows = self.data.shape[0]
        self.qtd_columns = self.data.shape[1]
        self.total_nulls = self.data.isnull().sum().sum()
        self.duplicated = self.data.duplicated().sum()
        self.columns = list(self.data.columns)
        
    def get_data_infos(self):
        return {
                'qtd_rows':self.qtd_rows,
                'qtd_columns':self.qtd_columns,
                'total_nulls':self.total_nulls,
                'total_duplicated':self.duplicated,
                '70_per_cent_null':self.check_null_values()
               }
        
    def check_null_values(self,
                          columns=None,
                          percentual_to_filter_null_columns = 0.7,
                          ):
        
        columns = columns if columns else self.columns
     
        null_columns = [(col,
                        round(1-self.data[col].count()/self.qtd_rows,1)) 
                        for col in columns
                        if (self.data[col].isnull().sum() >0) 
                        and (1-self.data[col].count()/self.qtd_rows) > percentual_to_filter_null_columns]
        
        return null_columns
        
    def verify_last_date(self,
                         column_date = None,
                         last_date = datetime.datetime.now()
                        ):
        
        if type(last_date) == type(datetime.datetime.now()):
            return pd.to_datetime(self.data[column_date]).sort_values(ascending = False).iloc[0] >= last_date
        else:
            try:
                last_date = datetime.datetime.strptime(last_date,'%d-%m-%Y')
                return pd.to_datetime(self.data[column_date]).sort_values(ascending = False).iloc[0] >= last_date
            except:
                print('Adjust format last date to dd-mm-yyyy')