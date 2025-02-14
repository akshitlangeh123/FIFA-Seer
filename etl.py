import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, regexp_replace

class fifa_seer:
    def __init__(self, app_name='Fifa_Seer'):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.warehouse.dir", "file:///D:/FIFA_SEER/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
        
    def extract(self, path):
        df = self.spark.read.csv(path, header=True, inferSchema=True)
        return df
    
    def transform(self, df):
        df = self.remove_unwanted_col(df)
        df = self.body_type_fix(df)
        df = self.gk_filter(df)
        df = self.fix_player_extra_val(df)
        df = self.fix_null_vals(df)
        return df
    
    def load(self, df, table_name):
        database_name, table = table_name.split(".")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        df.write.mode("overwrite") \
            .format("parquet") \
            .saveAsTable(table_name)
        print(f"Data successfully written to Hive table: {table_name}")
    
    def remove_unwanted_col(self, df):
        col_lst = ["sofifa_id","player_url","long_name","contract_valid_until","real_face","release_clause_eur","nation_jersey_number","loaned_from","nation_position","joined"]
        df = df.drop(*col_lst)
        return df

    def body_type_fix(self, df):
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

    def gk_filter(seld, df):
        df = df.filter(col('player_positions')!='GK')
        strng = "gk_diving|gk_handling|gk_kicking|gk_reflexes|gk_speed|gk_positioning|goalkeeping_diving|goalkeeping_handling|goalkeeping_kicking|goalkeeping_positioning|goalkeeping_reflexes"
        new_gk_lst = strng.split('|')
        df = df.drop(*new_gk_lst)
        return df

    def fix_player_extra_val(self, df):
        df = df.withColumn("plyer_tags",regexp_replace(col("player_tags"),"#",""))
        df = df.withColumn("player_tags",regexp_replace(col("player_tags")," (CPU AI Only)",""))
        return df

    def fix_null_vals(self, df):
        df = df.dropna(subset=['team_position','team_jersey_number'])
        df = df.fillna('not_specified')
        return df
    

if __name__ == "__main__":
    Fifa_seer_pipeline = fifa_seer()
    df = Fifa_seer_pipeline.extract("players_20.csv")
    transform_df = Fifa_seer_pipeline.transform(df)
    output_path = r"D:\FIFA_SEER\warehouse"
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    Fifa_seer_pipeline.load(transform_df, "my_database.fifa_players")

