import struct
import time

from dispersy.authentication import MemberAuthentication
from dispersy.callback import Callback
from dispersy.community import Community
from dispersy.conversion import DefaultConversion, BinaryConversion
from dispersy.destination import CommunityDestination
from dispersy.dispersy import Dispersy
from dispersy.distribution import FullSyncDistribution
from dispersy.endpoint import StandaloneEndpoint
from dispersy.message import Message, DropPacket, BatchConfiguration
from dispersy.payload import Payload
from dispersy.resolution import PublicResolution

class FloodCommunity(Community):
    def __init__(self, dispersy, master_member):
        super(FloodCommunity, self).__init__(dispersy, master_member)
        self.message_received = 0

    def initiate_conversions(self):
        return [DefaultConversion(self), FloodConversion(self)]

    def initiate_meta_messages(self):
        return [Message(self,
                        u"flood",
                        MemberAuthentication(encoding="sha1"),
                        PublicResolution(),
                        FullSyncDistribution(enable_sequence_number=False, synchronization_direction=u"ASC", priority=128),
                        CommunityDestination(node_count=22),
                        FloodPayload(),
                        self.check_flood,
                        self.on_flood,
                        batch=BatchConfiguration(3.0))]

    def create_flood(self, count):
        meta = self.get_meta_message(u"flood")
        messages = [meta.impl(authentication=(self.my_member,),
                              distribution=(self.claim_global_time(),),
                              payload=("flood #%d" % i,))
                    for i
                    in xrange(count)]
        self.dispersy.store_update_forward(messages, True, True, True)

    def check_flood(self, messages):
        for message in messages:
            yield message

    def on_flood(self, messages):
        self.message_received += len(messages)
        print "received %d messages (%d in batch)" % (self.message_received, len(messages))

class FloodPayload(Payload):
    class Implementation(Payload.Implementation):
        def __init__(self, meta, data):
            super(FloodPayload.Implementation, self).__init__(meta)
            self.data = data

class FloodConversion(BinaryConversion):
    def __init__(self, community):
        super(FloodConversion, self).__init__(community, "\x01")
        self.define_meta_message(chr(1), community.get_meta_message(u"flood"), self._encode_flood, self._decode_flood)

    def _encode_flood(self, message):
        return struct.pack("!L", len(message.payload.data)), message.payload.data

    def _decode_flood(self, placeholder, offset, data):
        if len(data) < offset + 4:
            raise DropPacket("Insufficient packet size")
        data_length, = struct.unpack_from("!L", data, offset)
        offset += 4

        if len(data) < offset + data_length:
            raise DropPacket("Insufficient packet size")
        data_payload = data[offset:offset + data_length]
        offset += data_length

        return offset, placeholder.meta.payload.implement(data_payload)

def join_flood_overlay(dispersy):
    master_member = dispersy.get_temporary_member_from_id("-FLOOD-OVERLAY-HASH-")
    my_member = dispersy.get_new_member()
    return FloodCommunity.join_community(dispersy, master_member, my_member)

def main():
    callback = Callback()
    endpoint = StandaloneEndpoint(10000)
    dispersy = Dispersy(callback, endpoint, u".", u":memory:")
    dispersy.start()
    print "Dispersy is listening on port %d" % dispersy.lan_address[1]

    community = callback.call(join_flood_overlay, (dispersy,))
    #callback.register(community.create_flood, (100,), delay=10.0)

    try:
        while callback.is_running:
            time.sleep(5.0)

            if community.message_received >= 10 * 100:
                time.sleep(60.0)
                break

    except KeyboardInterrupt:
        print "shutdown"

    finally:
        dispersy.stop()

if __name__ == "__main__":
    main()

