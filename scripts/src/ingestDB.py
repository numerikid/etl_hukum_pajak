import csv
import mysql.connector
from airflow.models import Variable
from datetime import datetime

date_now = datetime.now().strftime('%d-%m-%Y')



host = Variable.get("dwh_host")
username = Variable.get("dwh_user")
password = Variable.get("dwh_password")
database_name = Variable.get("dwh_database")
port = Variable.get("dwh_port")

def conDB():
    conn = mysql.connector.connect(host=host,user=username,port=port,password=password,database=database_name)
    return conn
def create_table():
    try:
        conn = conDB()
        if conn.is_connected():
            print("Terhubung ke database")
        cursor = conn.cursor()
        create_table_sql = """
                    CREATE TABLE IF NOT EXISTS news_pajak (
                        id_news bigint AUTO_INCREMENT PRIMARY KEY,
                        judul varchar(255),
                        link text NOT NULL,
                        tanggal varchar(255),
                        gambar text,
                        isi text,
                        portal varchar(255),
                        date_added timestamp);
                    """
        cursor.execute(create_table_sql)
        print("Tabel 'news_pajak' telah dibuat jika belum ada.")

    except mysql.connector.Error as e:
        print("Error:", e)

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Koneksi ke database ditutup")


def createLogs_table():
    try:
        conn = conDB()
        if conn.is_connected():
            print("Terhubung ke database")
        cursor = conn.cursor()
        create_table_sql = """
                    CREATE TABLE IF NOT EXISTS logscraping (
                            id_log bigint AUTO_INCREMENT PRIMARY KEY,
                            portal varchar(255),
                            tanggal varchar(255),
                            total bigint,
                            date_added timestamp
                                    );
                    """
        cursor.execute(create_table_sql)
        print("Tabel 'logscraping' telah dibuat jika belum ada.")
        
    except mysql.connector.Error as e:
        print("Error:", e)

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Koneksi ke database ditutup")

def load_to_dwh(types,path_files):
    if types == 'all':
        path_csv = f"{path_files}.csv"
    elif types == 'daily':      
        path_csv = f"{path_files}{date_now}.csv"
    else:path_csv = ""
    try:
        conn = conDB()
        cur = conn.cursor()
        with open(path_csv, "r") as f:
                reader = csv.reader(f)
                for row in reader:
                    judul,link,tanggal,gambar,isi,portal = row[0],row[1],row[2],row[3],row[4],row[5]
                    quer = """insert into news_pajak (judul,link,tanggal,gambar,isi,portal,date_added) values (%s,%s,%s,%s,%s,%s,now())"""
                    val = (judul,link,tanggal,gambar,isi,portal)
                    cur.execute(quer,val)
                conn.commit()
                cur.close()
                conn.close()
                print("Data loaded successfully!")
    except mysql.connector.Error as e:
        print("Error:", e)
    finally:
        if conn.is_connected():
            cur.close()
            conn.close()
            print("Koneksi ke database ditutup")

def load_logs_dwh(dates):
    try:
        conn = conDB()
        cur = conn.cursor()
        quer = """INSERT INTO logscraping (portal, tanggal, total, date_added)
                  SELECT portal, tanggal, COUNT(*) AS total, NOW()
                  FROM news_pajak
                  WHERE tanggal = %s
                  GROUP BY portal"""
        val = (dates,)
        cur.execute(quer, val)
        conn.commit()
        print("Data Logs loaded successfully!")
    except mysql.connector.Error as e:
        print("Error:", e)
    finally:
        if conn.is_connected():
            cur.close()
            conn.close()
            print("Koneksi ke database ditutup")
