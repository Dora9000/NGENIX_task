import csv
import os
import pathlib
import random
import shutil
from string import ascii_letters
from threading import Thread, Lock
import xml.etree.ElementTree as ET


class Creator(object):
    def __init__(self, path, num_arch=50, num_files=100):
        self.n = num_arch
        self.m = num_files
        self.path_to_result = path

    def make_xml(self, id_size=30):
        root = ET.Element("root")
        objects = ET.SubElement(root, "objects")
        id_ = ''.join(random.choice(ascii_letters) for _ in range(id_size))
        ET.SubElement(root, 'var').attrib = {'name': 'id', 'value': str(id_)}
        ET.SubElement(root, 'var').attrib = {'name': 'level', 'value': str(random.randint(1, 100))}
        for _ in range(random.randint(1, 10)):
            name = ''.join(random.choice(ascii_letters) for _ in range(id_size))
            ET.SubElement(objects, 'object', name=name)
        tree = ET.ElementTree(root)
        return tree

    def get_result(self):
        if not os.path.exists(self.path_to_result / 'tmp'):
            os.makedirs(self.path_to_result / 'tmp')
        for j in range(self.n):
            for i in range(self.m):
                self.make_xml().write(self.path_to_result / 'tmp' / "Result_{0}.xml".format(i))
            shutil.make_archive(self.path_to_result / "Result_{0}".format(j), 'zip', self.path_to_result / 'tmp')
            for f in os.listdir(self.path_to_result / 'tmp'):
                os.remove(self.path_to_result / 'tmp' / f)    
        os.rmdir(self.path_to_result / 'tmp')
        return self.path_to_result


class Parser(object):
    def __init__(self,
                 data_path,
                 extract_dir,
                 file1='file1.csv',
                 file2='file2.csv',
                 thread_number=8):
        self.data_path = data_path
        self.extract_dir = extract_dir
        self.file1 = file1
        self.file2 = file2
        self.N = thread_number

    def parse_xml(self, xml_file):
        tree = ET.ElementTree(file=xml_file)
        root = tree.getroot()
        objects = []
        id_ = level_ = ''
        for child in root:
            if child.tag == "objects":
                for step_child in child:
                    objects.append(step_child.attrib.get('name'))
                    
            if child.attrib.get('name') == "id":
                id_ = child.attrib.get('value')
            if child.attrib.get('name') == "level":
                level_ = child.attrib.get('value')
        return [id_, level_], [[id_, x] for x in objects]

    def parse_file(self, path_to_files, data):
        text1, text2 = self.parse_xml(path_to_files)
        data[0].acquire()
        data[1].append(text1)
        for s in text2:
            data[2].append(s)
        data[0].release()

    def work(self, data, files, worker_counter):
        while True:
            files[0].acquire()
            if len(files[1]) == 0:
                files[0].release()
                worker_counter[0] -= 1
                break
            else:
                filename = files[1][-1]
                del files[1][-1]
                files[0].release()
                self.parse_file(filename, data)

    def write(self, data, worker_counter):
        output_file1 = open(self.file1, 'w', newline='')
        writer1 = csv.writer(output_file1, delimiter=',')
        output_file2 = open(self.file2, 'w', newline='')
        writer2 = csv.writer(output_file2, delimiter=',')
        
        def write_data1(lines):
            for line in lines:
                writer1.writerow(line)
                
        def write_data2(lines):
            for line in lines:
                writer2.writerow(line)
                
        while True:
            data[0].acquire()
            write_data1(data[1])
            write_data2(data[2])
            data[1].clear()
            data[2].clear()
            data[0].release()
            if worker_counter[0] == 0:
                break
        output_file1.flush()
        output_file2.flush()
        output_file1.close()
        output_file2.close()

    def get_files(self, zip_dir, extract_dir):
        answer = []
        for arch in os.listdir(zip_dir):
            shutil.unpack_archive(zip_dir / arch, extract_dir / str(arch)[:-len('.zip')])
            for f in os.listdir(extract_dir / str(arch)[:-len('.zip')]):
                answer.append(extract_dir / str(arch)[:-len('.zip')] / f)
        return answer

    def parse(self):
        if not os.path.exists(self.extract_dir):
            os.makedirs(self.extract_dir)
            
        files = (Lock(), self.get_files(zip_dir=self.data_path, extract_dir=self.extract_dir))
        data = (Lock(), [], [])
        worker_counter = [self.N]
        threads = [Thread(target=self.work, args=(data, files, worker_counter)) for index in range(self.N)]
        for i in range(self.N):
            threads[i].start()
            
        writer = Thread(target=self.write, args=(data, worker_counter))
        writer.start()
        for i in range(self.N):
            threads[i].join()
        writer.join()


def delete_dir(path):
    if os.path.exists(path):
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(path)


if __name__ == '__main__':
    data_path = pathlib.Path(__file__).parent / 'XML'
    extract_dir = pathlib.Path(__file__).parent / 'extracted_XML'
    a = Creator(path=data_path, num_arch=50, num_files=100)
    a.get_result()

    b = Parser(data_path=data_path, extract_dir=extract_dir)
    b.parse()

    delete_dir(data_path)
    delete_dir(extract_dir)
