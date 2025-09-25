import json
import os
import psycopg2
import configparser


class Injest:
    def __init__(self):
        self.data = None  
        
    def process_data(self, data):
        processed_data = []
        for line in data:
            try:
                record = json.loads(line)
                processed_data.append(record)
            except json.JSONDecodeError as e:
                print(f"Erro ao decodificar JSON: {e}")
        return processed_data
        
    def save_to_db(self, processed_data):
        try:
            
            base_dir = os.path.dirname(os.path.abspath(__file__))  
            config_path = os.path.join(base_dir, "Config", "ConfigDb.ini")
            
            if config_path is None or not os.path.exists(config_path):
                print("❌ Config file not found.")
                return

            config = configparser.ConfigParser()
            config.read(config_path)

            if not config.sections():
                print("❌ No sections found.")
                return

            db_params = {
                "host": config["postgresql"]["host"],
                "port": config["postgresql"]["port"],
                "database": config["postgresql"]["database"],
                "user": config["postgresql"]["user"],
                "password": config["postgresql"]["password"],
            }

            # Conexão com Postgres
            conn = psycopg2.connect(**db_params)
            print("✅ Data base connected.")
            cursor = conn.cursor()

            # Cria tabela caso não exista
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data (                
                    id SERIAL PRIMARY KEY,
                    device_id VARCHAR(255) NOT NULL,
                    temp FLOAT NOT NULL,
                    ts TIMESTAMP NOT NULL
                )
            """)

            # Insere registros
            for record in processed_data:
                cursor.execute(
                    "INSERT INTO sensor_data (device_id, temp, ts) VALUES (%s, %s, %s)",
                    (record["device_id"], record["temp"], record["ts"])
                )

            conn.commit()
            cursor.close()
            conn.close()
            print("✅ Data saved successfully.")

        except Exception as e:
            print(f"❌ Error on saving data: {e}")

    def read_raw_data(self):
        with open("raw_data.json", "r") as f:
            return f.readlines()
    
    def run(self):
        raw_data = self.read_raw_data()
        processed_data = self.process_data(raw_data)
        print("✅ Data processed successfully.")
        
        self.save_to_db(processed_data)
        return processed_data
