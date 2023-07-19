import ujson as json
from mqtt_as import config
from mqtt_as import MQTTClient
from machine import Pin, SoftI2C
import ahtx0
import postgresql

ledRed = Pin(16, Pin.OUT)
i2c = SoftI2C(scl=Pin(5), sda=Pin(4), freq=100000)
sensor = ahtx0.AHT20(i2c)

config['ssid'] = 'FAMILIA REYES'
config['wifi_pw'] = 'Burbuja2001'

# MQTT configuration...
config['server'] = 'rat.rmq2.cloudamqp.com'
config['user'] = 'scecckcu:scecckcu'
config['password'] = 'UBQj1gLTSh-3NucCEbg-i5WhHLvtOiKA'

TOPIC1 = 'uleam/fcvt/grupo6'
TopicState = "hogar/cocina/luz/state"
TopicData = "hogar/cocina/luz/data"

def activaLed(topic, msg):
    if topic == "hogar/cocina/luz/state":
        StringData = str(msg, "utf-8") #quito el caracter del caracter especial
        print(StringData)
        if StringData != "offline":
            mData=json.loads(msg.decode())#Decode the JSON string
            #print(mData)
            if mData == True:
               ledRed.value(1)
            else:
               ledRed.value(0)
def mTimeStamp():
    import utime
    TimeSeconds=utime.time()
    mTimeStamp = 946684800 + TimeSeconds
    return mTimeStamp


def querymessage(nombre):
    latitud = -0.955748 # Manta
    longitud = -80.701301 # Manta
    try:
        temp=float('{0:.2f}'.format(sensor.temperature)) # random.randint(0,50)  
        hum = float('{0:.2f}'.format(sensor.relative_humidity)) 
        message = {
        "temperatura":temp,
        "humedad":hum,
        "sensor":nombre,
        "latitud":latitud,
        "longitud":longitud,
        "timestamp":mTimeStamp()}
    except OSError as e:
        print('Failed to read sensor.')
        message = {
        "temperatura":0,
        "humedad":0,
        "sensor":nombre,
        "latitud":latitud,
        "longitud":longitud,
        "fecha":mTimeStamp()}
    return message

def callback(topic, msg, retained):
    activaLed(topic, msg)
    print('[Consumidor-Micro] TOPIC: {}; Mensaje: {}'.format(topic, msg))
    
    # Save MQTT message to MongoDB
    data = {
        "topic": topic,
        "message": msg.decode(),
        "timestamp": mTimeStamp()
    }
    asyncio.create_task(save_mqtt_data(data))


async def save_sensor_data(data):
    # Conectarse a la base de datos PostgreSQL
# PostgreSQL configuration

    db = postgresql.open(
        host= 'containers-us-west-143.railway.app',
        port=7040,
        user="postgres",
        password="RB40eSXKi92UBpCSqtPP",
        database="railway"
    )

    # Preparar la sentencia de inserción
    make_sensor_data = db.prepare("INSERT INTO sensor (temperatura, humedad, sensor, latitud, longitud, timestamp) VALUES ($1, $2, $3, $4, $5, $6)")

    # Insertar el registro en la tabla
    make_sensor_data(
        data["temperatura"],
        data["humedad"],
        data["sensor"],
        data["latitud"],
        data["longitud"],
        data["fecha"]
    )

    # Cerrar la conexión
    db.close()

async def conn_han(client):
    # Resto del código (igual que antes) ...

async def main(client):
    # Resto del código (igual que antes) ...
    await client.connect()
    while True:
        await asyncio.sleep(5)
        
        # Publish sensor data to MQTT
        message = json.dumps(querymessage("WZM"))
        await client.publish(TOPIC1, message, qos=1)
        pixels.fill((255,255,0))
        pixels.write()
        await asyncio.sleep(1)
        pixels.fill((0,0,0))
        pixels.write()
        print('Mensaje publicado')


        # Save sensor data to PostgreSQL
        sensor_data = querymessage("WZM")
        await save_sensor_data(sensor_data)

        # Query data from PostgreSQL and publish to MQTT
        # Conectarse a la base de datos PostgreSQL
        db = postgresql.open(
           host= 'containers-us-west-143.railway.app',
        port=7040,
        user="postgres",
        password="RB40eSXKi92UBpCSqtPP",
        database="railway"
        )

        # Realizar una consulta SQL y publicar datos en MQTT
        cursor = db.cursor()
        cursor.execute("SELECT * FROM sensor")
        rows = cursor.fetchall()
        for row in rows:
            data = {
                "temperatura": row[0],
                "humedad": row[1],
                "sensor": row[2],
                "latitud": row[3],
                "longitud": row[4],
                "fecha": row[5]
            }
            message = json.dumps(data)
            await client.publish(TopicData, message, qos=1)

        # Cerrar la conexión
        db.close()

config['subs_cb'] = callback
config['connect_coro'] = conn_han
MQTTClient.DEBUG = True
client = MQTTClient(config)

try:
    asyncio.run(main(client))
finally:
    client.close()
