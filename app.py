from time import sleep
from manager import SiddhiAppManager
from query_callback import QueryCallbackImpl
from sender import EventSender

SIDDHI_APP = (
    "define stream cseEventStream (symbol string, price float, volume long); "
    "@info(name = 'query1') "
    "from cseEventStream[volume < 150] "
    "select symbol,price insert into outputStream;"
)

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
