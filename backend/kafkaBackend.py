# kafka_backend.py
from flask import Flask, request, jsonify
from flask_cors import CORS
from confluent_kafka import Producer, Consumer
import json
import threading
import requests

app = Flask(__name__)
CORS(app)

AIRFLOW_API = "http://3.14.252.252:8080/api/v1"
AIRFLOW_AUTH = ("airflow", "airflow")

class KafkaHandler:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()}')

    def publish(self, message):
        try:
            self.producer.produce(
                'airflow_triggers',
                json.dumps(message).encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.flush()
        except Exception as e:
            print(f"Error publishing message: {e}")
            raise

def trigger_airflow_dag(dag_id, conf):
    url = f"{AIRFLOW_API}/dags/{dag_id}/dagRuns"
    print(f"Triggering URL: {url} with conf: {conf}")
    try:
        response = requests.post(
            url,
            json={"conf": conf},
            auth=AIRFLOW_AUTH
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error triggering DAG: {e}")
        raise

def kafka_consumer():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'airflow_consumer',
        'auto.offset.reset': 'earliest'
    })
    
    consumer.subscribe(['airflow_triggers'])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                dag_id = data['dagId']
                
                for input_line in data['inputs']:
                    num1, num2 = input_line.split(',')
                    conf = {
                        "num1": num1.strip(),
                        "num2": num2.strip()
                    }
                    result = trigger_airflow_dag(dag_id, conf)
                    print(f"Triggered DAG {dag_id} with input {conf}: {result}")
                    
            except Exception as e:
                print(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Initialize Kafka handler
kafka_handler = KafkaHandler()

# Start consumer thread
consumer_thread = threading.Thread(target=kafka_consumer, daemon=True)
consumer_thread.start()

@app.route('/dags', methods=['GET'])
def get_dags():
    dags = ['user_input_2sum']
    return jsonify(dags)

@app.route('/trigger', methods=['POST'])
def trigger_dag():
    try:
        data = request.json
        print(f"Producing message: {data}")
        kafka_handler.publish(data)
        return jsonify({"status": "queued"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)