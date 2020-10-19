import os
import re
import time
import json
import pickle
import xml.sax
import multiprocessing
from threading import Thread
from wikireader import WikiReader


def extract_anchors():
    while not (finish and articles.empty()):

        title, text = articles.get()

        if re.search("{{[Rr]ozlišovacia stránka}}", text):

            anchors = re.findall("\\[\\[([^\\]\\|]+)\\|?([^\\[]+)?\\]\\]", text)
            print(anchors)
            processed_anchors.put(anchors)

        processed_articles.put(json.dumps({"page": title, "sentences": text}))


def write_to_file():
    while not (finish and processed_anchors.empty()):
        pickle.dump(processed_anchors.get(), out_file)


def print_status():
    while True:
        print("Articles: For processing: {0} For writing: {1} Read: {2}".format(
            articles.qsize(),
            processed_anchors.qsize(),
            reader.status_count))
        time.sleep(1)


if __name__ == "__main__":
    finish = False
    SRC_PATH = 'C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\data\\'
    OUTPUT_PATH = 'C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\output\\'
    SRC_FILE = 'test_sample.xml'
    OUTPUT = 'disambiguation_pages_content.p'

    wiki = os.path.join(SRC_PATH, SRC_FILE)
    out_file = open(os.path.join(OUTPUT_PATH, OUTPUT), "wb")

    manager = multiprocessing.Manager()
    articles = manager.Queue(maxsize=1000)
    processed_articles = manager.Queue(maxsize=1000)
    processed_anchors = manager.Queue()

    reader = WikiReader(lambda ns: ns == 0, articles.put)

    status_thread = Thread(target=print_status, args=())
    status_thread.start()

    threads = []
    for _ in range(2):
        threads = Thread(target=extract_anchors())
        threads.start()

    write_thread = Thread(target=write_to_file)
    write_thread.start()

    xml.sax.parse(wiki, reader)
    finish = True

    # print(processed_anchors)
