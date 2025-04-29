from time import sleep
from manager import SiddhiAppManager
from query_callback import QueryCallbackImpl
from sender import EventSender
from mqtt_stream import MQTTStream 
from siddhi_query import SiddhiQuery

# Criação dinâmica da definição de stream usando MQTTStream
mqtt_stream = MQTTStream("cseEventStream")
mqtt_stream.add_mqtt_attribute("mqtt_symbol", "string")
mqtt_stream.add_mqtt_attribute("mqtt_price", "float")
mqtt_stream.add_mqtt_attribute("mqtt_volume", "long")

# Criação da query como objeto
query = SiddhiQuery(
    name="query1",
    query_string=(
        "@info(name = 'query1') "
        "from cseEventStream[mqtt_volume < 150] "
        "select mqtt_symbol, mqtt_price insert into outputStream;"
    )
)


# 3. Geração da Siddhi App unindo definição de stream + query
SIDDHI_APP = str(mqtt_stream) + " " + str(query)


# SIDDHI_APP = (
#     "define stream cseEventStream (symbol string, price float, volume long); "
#     "@info(name = 'query1') "
#     "from cseEventStream[volume < 150] "
#     "select symbol,price insert into outputStream;"
# )

def main():
    manager = SiddhiAppManager(SIDDHI_APP)
    manager.add_callback(query.name, QueryCallbackImpl())
    input_handler = manager.get_input_handler(mqtt_stream.stream_name)

    sender = EventSender(input_handler, mqtt_stream)

    manager.start()
    sender.send_events()
    sleep(10)
    manager.shutdown()

if __name__ == "__main__":
    main()
