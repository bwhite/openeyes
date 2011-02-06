import random
import time
import tempfile


class JobTracker(object):
    """Manages data, task, and worker keys.  Dispatches jobs.
    """

    def __init__(self):
        self._tasks = {}  # Every instance of a task
        self._data_map = {}  # [key] = Data Location and State

    def create_data_input(self, user, url):
        """Creates data locations as input

        This is for all input options, the type is detected from the url.

        Args:
            user: User
            url: Url

        Returns:
            Opaque data key (str)
        """
        k = str(random.random())
        self._data_map[k] = {'type': 'url',
                             'data': url,
                             'state': 'ready',
                             'user': user}
        return k

    def create_data(self, user):
        """Creates data locations for job output

        This allows jobs to place data in a location that makes sense for it.

        Motivating Examples:  If the job is running on a GPU then the results
        may be held in GPU RAM for further processing, if run on a Hadoop
        cluster than it would be on HDFS, and if run on an external service it
        would be available through remote API calls.

        Args:
            user: User

        Returns:
            Opaque data key (str)
        """
        k = str(random.random())
        f = tempfile.NamedTemporaryFile(delete=False)
        self._data_map[k] = {'type': 'default',
                             'state': None,
                             'data': f.name,
                             'user': user}
        return k

    def get_data_state(self, user, input_key):
        """Query input key for data state

        Args:
            user: User
            input_key: Input key

        Return:
            Data state
        """
        return self._data_map[input_key]['state']

    def create_task(self, user, name, params, input_keys, output_keys):
        """
        Args:
            user: User
            name: Unique hashable id for the task to run
            params: Dict of parameters
            input_keys: Iterator of input keys
            output_keys: Iterator of output keys

        Returns:
            Opaque task key (str)
        """
        k = str(random.random())
        self._tasks[k] = {'user': user,
                          'name': name,
                          'params': params,
                          'time': time.time(),
                          'state': None,
                          'input_keys': list(input_keys),
                          'output_keys': list(output_keys)}
        return k

    def get_task_state(self, user, task_key):
        """Query input key for data state

        Args:
            user: User
            task_key: Task key

        Return:
            Task state
        """
        return self._tasks[task_key]['state']

    def get_available_task(self, worker_key):
        """Get a task if available

        Args:
            worker_key: Key for worker

        Returns:
           (task_key, data) or None
        """
        task_ready = lambda x: all(self._data_map[y]['state'] == 'ready'
                                   for y in x['input_keys'])
        try:
            t = ((x, y) for x, y in self._tasks.iteritems()
                 if task_ready(y) and not y['state']).next()
        except StopIteration:
            pass
        else:
            t[1]['state'] = 'running', worker_key
            return t[1]

    def get_data(self, input_key):
        """
        Args:
            input_key: Data input

        Returns:
            Data
        """
        return self._data_map[input_key]
