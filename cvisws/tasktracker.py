import classipy
import time
import urllib


class TaskTracker(object):

    def __init__(self, jobtracker):
        self._jobtracker = jobtracker
        # start thread that runs

    def _run(self):
        while 1:
            self.do_work()
            time.sleep(1)

    def do_work(self):
        import imfeat
        task = self._jobtracker.get_available_task()
        workables = dict(
            feature=self.do_task_feature,
            train=self.do_task_train)
        workables[task['type']](task)

    def do_task_feature(self, task):
        assert len(task.input_keys) == 1
        # Get the image
        #image = download(task['data']['url'])
        # Run feature
        # feature = imfeat.histogram_joint.make_feature(image)
        # store that shit

    def get_data(self, input_key):
        """
        Args:
            input_key: Data input

        Returns:
            Data
        """
        data = self._jobtracker.get_data(input_key)
        if data['type'] == 'url':
            # TODO: Do conversion check with modes
            return urllib.urlopen(data['data']).read()

    def do_task_train(self, task):
        s = classipy.SVMLinear(options=task['params'])
        out = s.train([self.get_data(x) for x in task['input_data']])
        
