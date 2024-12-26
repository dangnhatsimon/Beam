import apache_beam as beam
from apache_beam import coders
from apache_beam.coders import Coder
import unicode

coders.registry.get_coder(int)
coders.registry.register_coder(int, coders.VarIntCoder)


class StrUtf8Coder(Coder):
    def encode(self, value):
        return value.encode('utf-8')

    def decode(self, value):
        return value.decode('utf-8')

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return unicode


Coder.register_structured_urn(common_urns.coders.STRING_UTF8.urn, StrUtf8Coder)