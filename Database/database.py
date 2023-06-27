import sqlite3
import pandas as pd

class Database():
    
    def insert_into_table(df, tablename):
        con = sqlite3.connect("database.db")
        cur = con.cursor()
        df.to_sql(name=tablename, con=con, if_exists="", index=False)
        con.commit()
        cur.close()
        con.close()
    
    def get_df_from_db(query, tablename):
        con = sqlite3.connect("database.db")
        cur = con.cursor()
        df = pd.read_sql_query(f"select * from {tablename}", con)
        con.commit()
        cur.close()
        con.close()


if __name__ == "__main__":
    pass
    


