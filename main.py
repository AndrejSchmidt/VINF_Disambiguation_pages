import os
import re
import csv
import time
import xml.sax
import multiprocessing
from queue import Empty
from threading import Thread
from wikireader import WikiReader


def extract_anchors():
    while not (finish and articles.empty()):

        try:
            title, text = articles.get(timeout=2)
        except Empty:
            continue

        if re.search("{{[Rr]ozlišovacia stránka}}", text):

            anchors = re.findall("\\[\\[([^\\]\\|]+)\\|?([^\\[]+)?\\]\\]", text)
            # print(anchors)
            for anchor in anchors:
                line = list(anchor)
                line.insert(0, title)
                processed_anchors.put(line)
                # print(line)


def write_to_file():
    csv_writer = csv.writer(disambiguation_content, delimiter='\t')

    while not (finish and processed_anchors.empty()):
        try:
            anchors = processed_anchors.get(timeout=1)
        except Empty:
            continue

        csv_writer.writerow(anchors)


def print_status():
    while not finish:
        print("Articles: For processing: {0} For writing: {1} Read: {2}".format(
            articles.qsize(),
            processed_anchors.qsize(),
            reader.status_count))
        time.sleep(1)


if __name__ == "__main__":
    finish = False
    extracting_done = False
    SRC_PATH = 'C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\data\\'
    OUTPUT_PATH = 'C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\output\\'
    SRC_FILE = 'skwiki-latest-pages-articles.xml'
    DISAMBIGUATION_CONTENT = 'disambiguation_pages_content.csv'

    wiki = os.path.join(SRC_PATH, SRC_FILE)

    manager = multiprocessing.Manager()
    articles = manager.Queue(maxsize=1000)
    processed_articles = manager.Queue(maxsize=1000)
    processed_anchors = manager.Queue()

    reader = WikiReader(lambda ns: ns == 0, articles.put)

    with open(os.path.join(OUTPUT_PATH, DISAMBIGUATION_CONTENT), "w", newline='') as disambiguation_content:

        status_thread = Thread(target=print_status, args=())
        status_thread.start()

        threads = []
        for _ in range(1):
            threads.append(Thread(target=extract_anchors))
            threads[-1].start()

        write_thread = Thread(target=write_to_file)
        write_thread.start()

        xml.sax.parse(wiki, reader)
        finish = True

        status_thread.join()
        write_thread.join()
        for thread in threads:
            print("Joining thread {0}".format(thread.getName()))
            thread.join()
        print("Threads joined")
