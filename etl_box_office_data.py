from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("mysql+pymysql://root@localhost:3306/etl_box_office_data")

df = pd.read_csv("C:\\Xiver\\All Project\Data Engineer\\Top Movies Analyze - ETL\\enhanced_box_office_data(2000-2024)u.csv") 

# print(df.head())

df_cleaned = df.dropna()

df_cleaned = df_cleaned.drop(columns=["$Domestic", "$Foreign"])

print(df_cleaned.head())

# df_cleaned.to_csv("cleaned_dataset(Remove $Domestic, $Foreign column).csv", index=False) 


# Masukkan ke MySQL (tabel dibuat otomatis)
df_cleaned.to_sql("cleaned_data", con=engine, if_exists="replace", index=False)

print("Data berhasil dimasukkan ke database!")