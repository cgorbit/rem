import unittest
from rem.storages import LocalTag, TagWrapper


class T07(unittest.TestCase):
    """Checking internal REM structures"""

    def testTagWrapperSerialization(self):
        import cPickle

        tag = LocalTag("test", lambda *args: None)
        wrapOrig = TagWrapper(tag)
        wrapDesc = cPickle.dumps(wrapOrig)
        wrapNew = cPickle.loads(wrapDesc)
        self.assertTrue(isinstance(wrapNew, TagWrapper))
        self.assertEqual(wrapNew.name, wrapOrig.name)

