'''
Graded Challenge 7

Nama  : Rivaldi Valensia

Batch : FTDS-HCK-007

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai customer bank churn.
'''


# Membuat database db_phase2
CREATE DATABASE db_phase2
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

# Membuat tabel public.table_gc7
CREATE TABLE public.table_gc7
(
)
;

# Mengubah pemilik tabel menjadi postgres jika tabel sudah ada
ALTER TABLE IF EXISTS public.table_gc7
    OWNER to postgres;

# Membuat struktur tabel table_gc7
CREATE TABLE table_gc7 (
    "Customer Id" INT,
    "Credit Score" INT,
    "Country" VARCHAR(255),
    "Gender" VARCHAR(10),
    "Age" INT,
    "Tenure" INT,
    "Balance" FLOAT,
    "Products Number" INT,
    "Credit Card" INT,
    "Active Member" INT,
    "Estimated Salary" FLOAT,
    "Churn" INT
);

# Menampilkan semua data dari tabel table_gc7
SELECT * FROM table_gc7
