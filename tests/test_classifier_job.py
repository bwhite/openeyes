import unittest
import cvisws
import time


class Tets(unittest.TestCase):

    def setUp(self):
        self._jt = cvisws.JobTracker()
        self._tt = cvisws.TaskTracker(self._jt)
        self._u = 'user'

    def test_classifier_job(self):
        im_train_url = "/home/brandyn/playground/cvisws_test0/train_images.pkl"
        im_test_url = "/home/brandyn/playground/cvisws_test0/test_images.pkl"
        gt_url = "/home/brandyn/playground/cvisws_test0/train_gt.pkl"

        # Set up all the input/output data nodes
        key_im_train = self._jt.create_data_input(self._u, im_train_url)
        key_im_test = self._jt.create_data_input(self._u, im_test_url)
        key_feat_train = self._jt.create_data(self._u)
        key_feat_test = self._jt.create_data(self._u)
        key_gt = self._jt.create_data_input(self._u, gt_url)
        key_classifier = self._jt.create_data(self._u)
        key_predict = self._jt.create_data(self._u)

        # Create all the jobs
        tkey_feat_train = self._jt.create_task(self._u, 'feature', {},
                                [key_im_train], [key_feat_train])

        tkey_feat_test = self._jt.create_task(self._u, 'feature', {},
                                [key_im_test], [key_feat_test])

        tkey_train = self._jt.create_task(self._u, 'train', {},
                                [key_feat_train, key_gt], [key_classifier])

        tkey_predict = self._jt.create_task(self._u, 'predict', {},
                                [key_feat_test, key_classifier], [key_predict])
        while 1:
            try:
                self._tt.do_work()
            except TypeError, e:
                print(e)
                break
        print(self._jt.get_data(key_predict)['data'])
        return
        # Wait patiently for jobs to complete
        tkeys = [tkey_feat_train, tkey_feat_test, tkey_train, tkey_predict]
        for tkey in tkeys:
            while True:
                state = self._jt.get_task_state(self._u, tkey_predict)
                print 'time: %f key: %s state: %s' % (time.time(),
                                                      tkey, str(state))
                if state and state['done']:
                    break
                time.sleep(1)


if __name__ == '__main__':
    unittest.main()
