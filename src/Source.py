import json, time, random, datetime

# Classe simulates a data source generating JSON data from a PLC
class Source:
    def generate(self):
        while True:
            data = {
                "device_id": "PLC",
                "temp": round(random.uniform(60, 100), 2),
                "ts": datetime.datetime.utcnow().isoformat()
            }
            with open("raw_data.json", "a") as f:
                f.write(json.dumps(data) + "\n")
            print("Novo dado gerado:", data)
            time.sleep(2)
    