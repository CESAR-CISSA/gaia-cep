from PySiddhi.DataTypes.LongType import LongType

class EventSender:
    def __init__(self, input_handler):
        self.input_handler = input_handler

    def send_events(self):
        events = [
            ["IBM", 700.0, LongType(100)],
            ["WSO2", 60.5, LongType(200)],
            ["GOOG", 50, LongType(30)],
            ["IBM", 76.6, LongType(400)],
            ["WSO2", 45.6, LongType(50)]
        ]
        for event in events:
            self.input_handler.send(event)
