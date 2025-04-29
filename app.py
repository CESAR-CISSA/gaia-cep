from time import sleep
from manager import SiddhiAppManager
from query_callback import QueryCallbackImpl
from sender import EventSender
from mqtt_stream import MQTTStream 

# Criação dinâmica da definição de stream usando MQTTStream
mqtt_stream = MQTTStream("cseEventStream")
mqtt_stream.add_mqtt_attribute("mqtt_symbol", "string")
mqtt_stream.add_mqtt_attribute("mqtt_price", "float")
mqtt_stream.add_mqtt_attribute("mqtt_volume", "long")

# Geração da string Siddhi com stream dinâmico
stream_definition = mqtt_stream.defineStreamString()
print(stream_definition);
SIDDHI_APP = (
    stream_definition + "; "

    "@info(name = 'query1') "
    "from cseEventStream[volume < 150] "
    "select symbol,price insert into outputStream;"
)


# SIDDHI_APP = (
#     "define stream cseEventStream (symbol string, price float, volume long); "
#     "@info(name = 'query1') "
#     "from cseEventStream[volume < 150] "
#     "select symbol,price insert into outputStream;"
# )

def main():
    manager = SiddhiAppManager(SIDDHI_APP)
    manager.add_callback("query1", QueryCallbackImpl())
    input_handler = manager.get_input_handler("cseEventStream")

    sender = EventSender(input_handler)

    manager.start()
    sender.send_events()
    sleep(10)
    manager.shutdown()

if __name__ == "__main__":
    main()
