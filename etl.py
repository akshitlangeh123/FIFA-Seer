from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, regexp_replace

def remove_unwanted_col(df,col_lst):
    df = df.drop(*col_lst)
    return df

def body_type_fix(df):
    df = df.withColumn("body_type", 
                     when(df['body_type']=='Shaqiri','Normal')
                     .when(df['body_type']=='PLAYER_BODY_TYPE_5','Normal')
                     .when(df['body_type']=='Neymar','Lean')
                     .when(df['body_type']=='C. Ronaldo','Normal')
                     .when(df['body_type']=='Courtois','Normal')
                     .when(df['body_type']=='Messi','Normal')
                     .when(df['body_type']=='Akinfenwa','Stocky')
                     .otherwise(df['body_type']))
    return df

def gk_filter(df):
    df = df.filter(col('player_positions')=='GK')
    strng = "gk_diving|gk_handling|gk_kicking|gk_reflexes|gk_speed|gk_positioning|goalkeeping_diving|goalkeeping_handling|goalkeeping_kicking|goalkeeping_positioning|goalkeeping_reflexes"
    new_gk_lst = strng.split('|')
    df2 = df2.drop(*new_gk_lst)
    return df

