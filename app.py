from PySiddhi.core.SiddhiManager import SiddhiManager
from mqtt_stream import MQTTStream
from query_callback import QueryCallbackImpl
from siddhi_query import SiddhiQuery
from sender import EventSender
from time import sleep
import asyncio

async def main():
    # Instancia o stream MQTT
    mqtt_stream = MQTTStream("cseEventStream")
    mqtt_stream.add_mqtt_attribute("mqtt_messagetype", "int")
    mqtt_stream.add_mqtt_attribute("mqtt_messagelength", "long")
    mqtt_stream.add_mqtt_attribute("mqtt_flag_qos", "int")

    # Define a query Siddhi
    query = SiddhiQuery(
        name="query1",
        query_string="@info(name = 'query1') " +
        "from cseEventStream[ mqtt_messagelength < 150 ] " +
        "select mqtt_messagetype, mqtt_messagelength, mqtt_flag_qos insert into outputStream;"
    )

    siddhi_manager = SiddhiManager()
    siddhi_app = str(mqtt_stream) + " " + str(query)
    siddhi_runtime = siddhi_manager.createSiddhiAppRuntime(siddhi_app)

    siddhi_runtime.addCallback(query.name, QueryCallbackImpl())
    input_handler = siddhi_runtime.getInputHandler(mqtt_stream.stream_name)

    sender = EventSender(input_handler, mqtt_stream)
    siddhi_runtime.start()

    await sender.send_event_from_csv("./data/eventos.csv")

    sleep(5)
    siddhi_manager.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
