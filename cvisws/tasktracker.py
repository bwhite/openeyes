import classipy
import cPickle as pickle
import cStringIO as StringIO
import Image
import imfeat
import random


class TaskTracker(object):

    def __init__(self, jobtracker):
        self._jt = jobtracker
        self._worker_key = str(random.random())

    def do_work(self):
        task = self._jt.get_available_task(self._worker_key)
        workables = dict(
            feature=self.do_task_feature,
            train=self.do_task_train,
            predict=self.do_task_predict)
        input_data = [pickle.load(open(self._jt.get_data(i)['data']))
                      for i in task['input_keys']]
        print(task['name'])
        out = workables[task['name']](task, input_data)
        pickle_fn = self._jt.get_data(task['output_keys'][0])['data']
        with open(pickle_fn, 'w') as fp:
            pickle.dump(out, fp, -1)
        self._jt.get_data(task['output_keys'][0])['state'] = 'ready'

    def do_task_feature(self, task, input_data):
        name_images, = input_data
        out = []
        for name, image in name_images[:10]:
            image = Image.open(StringIO.StringIO(image))
            feat = imfeat.histogram_joint.make_features(image)[0]
            out.append((name, feat))
        return out

    def do_task_train(self, task, input_data):
        name_features, name_gts = input_data
        name_gts = dict(name_gts)
        class_data = {}  # [class_name] = [(label, value) ...]
        for name, feature in name_features:
            poss, negs = name_gts[name]
            for pos in poss:
                class_data.setdefault(pos, []).append((1, feature))
            for neg in negs:
                class_data.setdefault(neg, []).append((-1, feature))
        out = []
        for class_name, data in class_data.items():
            s = classipy.SVMLinear(options=task['params']).train(data)
            out.append((class_name, s.dumps()))
        return out

    def do_task_predict(self, task, input_data):
        name_features, classifiers = input_data
        classifiers = [(x, classipy.SVMLinear.loads(y))
                       for x, y in classifiers]
        out = {}  # [name][class_name] = conf
        for name, feature in name_features:
            for class_name, classifier in classifiers:
                a, = classifier.predict(feature)
                out.setdefault(name, {})[class_name] = a[0] * a[1]
        return out
