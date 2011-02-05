import unittest
import cvisws


class Test(unittest.TestCase):

    def setUp(self):
        self._jt = cvisws.JobTracker()
        self._u = 'user here'

    def test_data0(self):
        k = self._jt.create_data(self._u)
        self.assertEquals(self._jt.get_data_state(self._u, k), None)
        self.assertEquals(self._jt.get_data_state(self._u, k),
                          self._jt.get_data(k)['state'])

    def test_data1(self):
        k = self._jt.create_data_input(self._u, 'http://myimage.com/d.jpg')
        self.assertEquals(self._jt.get_data_state(self._u, k), 'ready')
        self.assertEquals(self._jt.get_data_state(self._u, k),
                          self._jt.get_data(k)['state'])

    def test_job(self):
        k = self._jt.create_data_input(self._u, 'http://myimage.com/d.jpg')
        name = 'task_name'
        task_key_orig = self._jt.create_task(self._u, name, {}, [k], [])
        worker_key = 'myworkerkey'
        task_key, task_data = self._jt.get_available_task(worker_key)
        self.assertEquals(task_key_orig, task_key)
        self.assertEquals(task_data['state'], ('running', worker_key))
        self.assertEquals(task_data['state'],
                          self._jt.get_task_state(self._u, task_key))
        self.assertEquals(task_data['user'], self._u)
        self.assertEquals(task_data['name'], name)
        self.assertTrue('time' in task_data)
        self.assertEquals(task_data['params'], {})
        self.assertEquals(task_data['input_keys'], [k])
        self.assertEquals(task_data['output_keys'], [])


if __name__ == '__main__':
    unittest.main()
