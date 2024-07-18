from flask import Flask, request, jsonify, render_template, Response
from confluent_kafka import Producer, Consumer, KafkaError
import threading
import time
import json


app = Flask(__name__)

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'live-score-topic'

producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'score-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])

scores = {"India": 0, "Pakistan": 0}

def delivery_report(err, msg):
    """Delivery report callback called once for each message produced."""
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

@app.route('/update_score', methods=['POST'])
def update_score():
    print(request.json)
    data = request.json
    team = data.get('team')
    score = data.get('score')
    
    if team and score is not None:
        scores[team] = score
        producer.produce(KAFKA_TOPIC, value=str(scores).encode('utf-8'), callback=delivery_report)
        producer.flush()
        return jsonify({'status': 'Score updated', 'scores': scores}), 200
    else:
        return jsonify({'status': 'Invalid request'}), 400

@app.route('/get_scores', methods=['GET'])
def get_scores():
    return jsonify(scores), 200

def consume_scores():
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                continue
        scores.update(eval(msg.value().decode('utf-8')))

@app.route('/score_stream')
def score_stream():
    def generate():
        while True:
            time.sleep(1)
            yield f"data: {json.dumps(scores)}\n\n"
    return Response(generate(), mimetype='text/event-stream')

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    threading.Thread(target=consume_scores).start()
    app.run(debug=True)
