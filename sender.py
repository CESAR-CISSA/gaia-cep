from PySiddhi.DataTypes.LongType import LongType
from PySiddhi.DataTypes.DoubleType import DoubleType


from mqtt_stream import MQTTStream

class EventSender:
    def __init__(self, input_handler, stream: MQTTStream):
        self.input_handler = input_handler
        self.stream = stream

    def send_events(self):
        """
        Envia eventos com base na definição de atributos da stream.
        Os valores são definidos manualmente aqui como exemplo.
        """
        # Lista de eventos simulados com os valores na ordem dos atributos definidos
        sample_values = [
            ["IBM", 700.0, 100],
            ["WSO2", 60.5, 200],
            ["GOOG", 50.0, 30],
            ["IBM", 76.6, 400],
            ["WSO2", 45.6, 50],
        ]

        attribute_order = self.stream.get_attribute_names()

        for values in sample_values:
            event = []
            for idx, attr_name in enumerate(attribute_order):
                tipo = self.stream.get_attribute_type(attr_name)
                val = values[idx]
                if tipo == "long":
                    event.append(LongType(val))
                else:
                    event.append(val)
            self.input_handler.send(event)
