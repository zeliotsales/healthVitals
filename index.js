const { Kafka } = require('kafkajs');
require('dotenv').config();


const patientNames = {
  1: 'Tharanesh',
  2: 'Akshay',
  3: 'Sridhar',
  4: 'Tanmaya',
  5: 'Sugam'
};

const INTERVAL_SEC = 5;
const KAFKA_OUTPUT_TOPIC = process.env.KAFKA_OUTPUT_TOPIC ;


const kafka = new Kafka({
  clientId: 'vitals-producer',
  brokers: ['my-cluster-kafka-bootstrap.kafka.svc:9092'],
  ssl: false,
  sasl: {
    mechanism: 'scram-sha-512',
    username: "my0jpefb0c6q7u8fkfeqjb0t7",
    password: "a8438SL6T2Vx4FJTuntCtzyTV5vl56JO"
  }
});

const producer = kafka.producer();

function getRandomValue(rand, min, max) {
  return Math.floor(rand * (max - min + 1)) + min;
}

function getRandomValueDouble(rand, min, max) {
  return Math.round((rand * (max - min) + min) * 10) / 10;
}

function getRandomValueOutOfRange(rand, min, max, isFloat = false) {
  if (Math.random() < 0.5) {
    return isFloat
      ? Math.round((min - (Math.random() * 5 + 0.1)) * 10) / 10
      : min - (Math.floor(Math.random() * 10) + 1);
  } else {
    return isFloat
      ? Math.round((max + (Math.random() * 5 + 0.1)) * 10) / 10
      : max + (Math.floor(Math.random() * 10) + 1);
  }
}

function generateVitals(isAlert) {
  const key = getRandomValue(Math.random(), 1, 5);
  const timestamp = Math.floor(Date.now() / 1000);
  const rand = Math.random;

  const vitals = {
    time: timestamp,
    Id: key,
    Name: patientNames[key] || 'Unknown Patient',
    body_temperature_F: isAlert
      ? getRandomValueOutOfRange(rand(), 96.0, 100.1, true)
      : getRandomValueDouble(rand(), 96.0, 100.0),
    heart_rate_bpm: isAlert
      ? getRandomValueOutOfRange(rand(), 60, 100)
      : getRandomValue(rand(), 60, 100),
    systolic_pressure_mmHg: isAlert
      ? getRandomValueOutOfRange(rand(), 90, 120)
      : getRandomValue(rand(), 90, 120),
    diastolic_pressure_mmHg: isAlert
      ? getRandomValueOutOfRange(rand(), 40, 120)
      : getRandomValue(rand(), 40, 120),
    respiratory_rate_breaths_per_min: isAlert
      ? getRandomValueOutOfRange(rand(), 5, 25)
      : getRandomValue(rand(), 12, 20),
    oxygen_saturation_percent: isAlert
      ? getRandomValue(rand(), 70, 90)
      : getRandomValue(rand(), 90, 100),
    blood_glucose_level_mg_per_dL: isAlert
      ? getRandomValueOutOfRange(rand(), 70, 120)
      : getRandomValue(rand(), 70, 120),
    etco2_mmHg: isAlert
      ? getRandomValueOutOfRange(rand(), 20, 60)
      : getRandomValue(rand(), 35, 45),
    skin_turgor_recoilTimeSec: isAlert
      ? getRandomValueOutOfRange(rand(), 5, 10)
      : getRandomValue(rand(), 1, 3),
    isAlert: isAlert
  };

  return { key: String(key), value: JSON.stringify(vitals) };
}

async function runProducer() {
  await producer.connect();
  console.log(`Producer connected. Sending to topic: ${KAFKA_OUTPUT_TOPIC}`);

  while (true) {
    const alertVitals = generateVitals(true);
    await producer.send({
      topic: KAFKA_OUTPUT_TOPIC,
      messages: [alertVitals]
    });
    console.log(`Key: ${alertVitals.key} Alert Vitals Sent:\n`, alertVitals.value);
    await sleep(INTERVAL_SEC * 1000);

    const normalVitals = generateVitals(false);
    await producer.send({
      topic: KAFKA_OUTPUT_TOPIC,
      messages: [normalVitals]
    });
    console.log(`Key: ${normalVitals.key} Normal Vitals Sent:\n`, normalVitals.value);
    await sleep(INTERVAL_SEC * 1000);
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

runProducer().catch(e => {
  console.error('Error running producer', e);
  process.exit(1);
});
