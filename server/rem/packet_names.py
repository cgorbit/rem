from rem.callbacks import ICallbackAcceptor
from rem.packet import PacketState, PacketBase
import rem.fork_locking as fork_locking


class PacketNamesStorage(ICallbackAcceptor):
    def __init__(self):
        self.packets = {}

    def __getstate__(self):
        return {}

    def Add(self, pck):
        self.packets[pck.name] = pck
        pck.AddCallbackListener(self)

    def Exist(self, pck_name):
        return pck_name in self.packets

    def Delete(self, pck_name):
        self.packets.pop(pck_name, None)

    def Get(self, pck_name):
        return self.packets.get(pck_name)

    def OnChange(self, pck):
        if isinstance(pck, PacketBase) and pck.state == PacketState.HISTORIED:
            self.Delete(pck.name)

    def OnJobDone(self, job_ref):
        pass

    def OnJobGet(self, job_ref):
        pass

    def OnPacketReinitRequest(self, pck):
        pass


import rem.storages
rem.storages.PacketNamesStorage = PacketNamesStorage
