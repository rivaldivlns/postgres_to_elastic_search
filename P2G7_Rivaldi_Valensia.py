'''
Graded Challenge 7

Nama  : Rivaldi Valensia

Batch : FTDS-HCK-007

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai customer bank churn.
'''

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

# String koneksi untuk PostgreSQL
conn_string = "dbname='db_phase2' host='localhost' user='postgres' password='valdi'"
conn = db.connect(conn_string)

# Membaca data dari tabel 'table_gc7' ke dalam DataFrame
df = pd.read_sql("select * from table_gc7", conn)

# Kolom yang akan digunakan
column = ['Customer Id', 'Credit Score', 'Country', 'Gender', 'Age', 
          'Tenure', 'Balance', 'Products Number', 'Credit Card',
          'Active Member', 'Estimated Salary', 'Churn']

# Fungsi untuk membersihkan data
def cleaning_data(dataframe):
    # Mengubah nama kolom menjadi lowercase
    dataframe.columns = [col.lower() for col in dataframe.columns]
    
    # Mengganti spasi dengan underscore pada nama kolom
    dataframe.columns = dataframe.columns.str.replace(' ', '_')
    
    # Mengganti nilai churn: 0 menjadi 'Retention', 1 menjadi 'Churn'
    dataframe['churn'] = dataframe['churn'].replace({0: 'Retention', 1: 'Churn'})
    
    # Mengonversi tipe data kolom 'balance' menjadi integer
    dataframe['balance'] = dataframe['balance'].astype(int)
    
    # Mengonversi tipe data kolom 'estimated_salary' menjadi integer
    dataframe['estimated_salary'] = dataframe['estimated_salary'].astype(int)
    
    # Menghapus missing values (NaN)
    dataframe.dropna(inplace=True)
    
    # Mengisi missing values with 0
    dataframe.fillna(0, inplace=True)
    
    # Mengisi missing values with mean of each column
    dataframe.fillna(dataframe.mean(), inplace=True)
    
        # Menghitung total penjualan dari kolom yang ada
    dataframe['total_sales'] = dataframe['balance'] + dataframe['estimated_salary']
    
    # Mengklasifikasikan total penjualan menjadi kategori berdasarkan nilai
    def classify_sales(total_sales):
        if total_sales < 10000:
            return 'Loss'
        elif total_sales == 10000:
            return 'Break-even'
        else:
            return 'Profit'
    
    # Menerapkan fungsi klasifikasi ke setiap baris dalam kolom 'total_sales'
    dataframe['sales_category'] = dataframe['total_sales'].apply(classify_sales)
    
    # Mengembalikan dataframe yang telah dibersihkan
    return dataframe

# Memanggil fungsi cleaning_data untuk membersihkan dataframe
cleaned_df = cleaning_data(df)

# Menyimpan dataframe yang telah dibersihkan ke file CSV
cleaned_df.to_csv('P2G7_Rivaldi_Valensia_data_clean.csv', index=False)
print("-------Data Saved------")

# Menghubungkan ke Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Membaca dataframe yang telah dibersihkan dari file CSV
df_cleaned = pd.read_csv('P2G7_Rivaldi_Valensia_data_clean.csv')

# Mengindeks setiap baris dari dataframe ke elasticsearch
for i, r in df.iterrows():
    # Mengonversi baris ke JSON
    doc = r.to_json()
    # Mengindeks data ke Elasticsearch
    res = es.index(index="tabel_gc7", body=doc)  
    print(res)
