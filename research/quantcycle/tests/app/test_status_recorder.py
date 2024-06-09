import unittest
from quantcycle.app.status_recorder.status_recorder import BaseStatusRecorder
import os


class pms:
    def __init__(self, pnl):
        self.pnl = pnl


class Teststatusrocorder(unittest.TestCase):

    def test_1(self):
        pms1 = pms(200)
        pms2 = pms(500)
        dict = {'1': pms1, '2': pms2}

        status = "pms_at_t1"
        path = ''

        status_recorder = BaseStatusRecorder()

        status_recorder.dump(dict, status, path)
        dic = status_recorder.load(status, path)

        assert dic['1'].pnl == dict['1'].pnl
        self.assertTrue(True)

    def test_2(self):
        pms1 = pms(0)
        pms2 = pms(-40)
        dict = {'1': pms1, '2': pms2}

        status = "pms_at_t2"
        path = path1=os.path.abspath('.')
        status_recorder = BaseStatusRecorder()

        status_recorder.dump(dict, status, path)
        dic = status_recorder.load(status, path)

        assert dic['2'].pnl == dict['2'].pnl
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
