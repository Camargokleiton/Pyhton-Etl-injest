
import json
import psycopg2
import configparser


class Injest:
    def __init__(self, data):
        self.data = data

    def process_data(self):
        processed_data = []
        for line in self.data:
            try:
                record = json.loads(line)
                processed_data.append(record)
            except json.JSONDecodeError as e:
                print(f"Erro ao decodificar JSON: {e}")
        return processed_data
        
    def save_to_db(self, processed_data):
        try:
            config = configparser.ConfigParser()
            config.read('Config/ConfigDb.ini')
            db_params = {
                            "host": config["postgresql"]["host"],
                            "port": config["postgresql"]["port"],
                            "database": config["postgresql"]["database"],
                            "user": config["postgresql"]["user"],
                            "password": config["postgresql"]["password"],
                        }
            conn = psycopg2.connect(**db_params)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data (                
                    id SERIAL PRIMARY KEY,
                    device_id VARCHAR(255) NOT NULL,
                    temp FLOAT NOT NULL,
                    ts TIMESTAMP NOT NULL
                )
            """)

            for record in processed_data:
                cursor.execute(
                    "INSERT INTO sensor_data (device_id, temp, ts) VALUES (%s, %s, %s)",
                    (record["device_id"], record["temp"], record["ts"])
                )
            conn.commit()
            cursor.close()
            conn.close()
            print("Dados salvos no banco de dados com sucesso.")
        except Exception as e:
            print(f"Erro ao salvar dados no banco de dados: {e}")
    def read_raw_data(self):
        with open("raw_data.json", "r") as f:
            return f.readlines()
    
    def run(self):
        raw_data = self.read_raw_data()
        processed_data = self.process_data()
        self.save_to_db(processed_data)
        return processed_data