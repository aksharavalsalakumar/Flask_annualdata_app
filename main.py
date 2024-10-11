from flask import Flask, request, jsonify
import pandas as pd
import sqlite3

app = Flask(__name__)

# To get the db connection
def get_db_connection():
    return sqlite3.connect('database.db')

#To create the annualdata table in db
def setup_database():
    with get_db_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS annualdata (
                API_WELL_NUMBER INTEGER PRIMARY KEY,
                OIL INTEGER,
                GAS INTEGER,
                BRINE INTEGER
            )
        """)
        conn.commit()

#To calculate the annual data and add the result to db
def load_data():
    df = pd.read_excel("data.xls").groupby("API WELL  NUMBER")[['OIL', 'GAS', 'BRINE']].sum().reset_index()
    data = df.values.tolist()

    with get_db_connection() as conn:
        conn.executemany("""
            INSERT INTO annualdata (API_WELL_NUMBER, OIL, GAS, BRINE)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(API_WELL_NUMBER) DO UPDATE SET
                OIL=excluded.OIL, GAS=excluded.GAS, BRINE=excluded.BRINE
        """, data)
        conn.commit()

 #To get the details corresponding to a specific api well number.
@app.route("/data", methods=["GET"])
def get_well_data():
    api_well_number = request.args.get("well")
    conn = get_db_connection()
    well_data = conn.execute("SELECT OIL, GAS, BRINE FROM annualdata WHERE API_WELL_NUMBER = ?",
                             (api_well_number,)).fetchone()
    conn.close()

    if well_data:
        return jsonify({"OIL": well_data[0], "GAS": well_data[1], "BRINE": well_data[2]})
    return jsonify({"error": "API well number not found"}), 404


if __name__ == "__main__":
    setup_database()
    load_data()
    app.run(port=8080)
