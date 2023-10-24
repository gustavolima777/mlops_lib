import pyarrow 
import time
import datetime
import pandas as pd
from functools import reduce

class DataQualityUtils:
    
    def __init__(self,
                 pyarrow_table):
        
        self.data = pyarrow_table
        self.qtd_rows = self.data.shape[0]
        self.qtd_columns = self.data.shape[1]
        self.columns = self.data.schema.names
        
    def get_data_infos(self):
        return {
                'qtd_rows':self.qtd_rows,
                'qtd_columns':self.qtd_columns,
                'total_nulls':self.sum_null_values()[0],
                '70_per_cent_null':self.sum_null_values()[1]
               }
        
    def sum_null_values(self, per_cent_null=0.7):
        
        nulls_ = []
        nulls_columns_ = []
        for col in self.data.schema:

            column_data = self.data[col.name].to_numpy()
            if self.data[col.name].type == 'string':
                nulls = np.sum(self.data[col.name] == None)
                nulls_.append(nulls)
                if (nulls > 0) and (nulls/self.qtd_rows >= per_cent_null):
                    nulls_columns_.append(col.name)
            else:    
                try:
                    nulls = np.isnan(column_data).sum()
                    nulls_.append(nulls)
                    if (nulls > 0) and (nulls/self.qtd_rows >= per_cent_null):
                        nulls_columns_.append(col.name)
                except:
                    pass
                
        return reduce(lambda x,y: x+y,nulls_),nulls_columns_
        
    def verify_last_date(self,
                         column_date = None,
                         last_date = datetime.datetime.now()
                        ):
        
        if type(last_date) == type(datetime.datetime.now()):
            return pd.to_datetime(self.data[column_date].to_pandas()).sort_values(ascending = False).iloc[0] >= last_date
        else:
            try:
                last_date = datetime.datetime.strptime(last_date,'%d-%m-%Y')
                return pd.to_datetime(self.data[column_date].to_pandas()).sort_values(ascending = False).iloc[0] >= last_date
            except:
                print('Adjust format last date to dd-mm-yyyy')
                