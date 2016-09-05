from rem.callbacks import ICallbackAcceptor
from rem.packet import PacketState, PacketBase
import rem.fork_locking as fork_locking


class PacketNamesStorage(ICallbackAcceptor):
    def __init__(self, *args, **kwargs):
        self.names = set(kwargs.get('names_list', []))
        self.lock = fork_locking.Lock()

    def __getstate__(self):
        return {}

    def Add(self, pck_name):
        with self.lock:
            self.names.add(pck_name)

    def Update(self, names_list=None):
        with self.lock:
            self.names.update(names_list or [])

    def Exist(self, pck_name):
        return pck_name in self.names

    def Delete(self, pck_name):
        with self.lock:
            if pck_name in self.names:
                self.names.remove(pck_name)

    def OnChange(self, packet_ref):
        if isinstance(packet_ref, PacketBase) and packet_ref.state == PacketState.HISTORIED:
            self.Delete(packet_ref.name)

    def OnJobDone(self, job_ref):
        pass

    def OnJobGet(self, job_ref):
        pass

    def OnPacketReinitRequest(self, pck):
        pass


import rem.storages
rem.storages.PacketNamesStorage = PacketNamesStorage
