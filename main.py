import difflib
import os
import re
import csv
import time
import xml.sax
import multiprocessing
from multiprocessing.context import Process
from queue import Empty
from threading import Thread
from wikireader import WikiReader


def count_anchor_similarity(anchor, anchor_text):
    seq = difflib.SequenceMatcher(None, anchor, anchor_text)
    similarity = seq.real_quick_ratio()

    # print([anchor, anchor_text, similarity])
    return similarity


def extract_anchors(r, articles, processed_anchors):
    finish = False

    while not (finish and articles.empty()):
        if r.poll() is True:
            finish = True

        try:
            title, text = articles.get(timeout=2)
        except Empty:
            continue

        if re.search("{{[Rr]ozlišovacia stránka}}", text):

            anchors = re.findall("\\[\\[([^\\]\\|]+)\\|?([^\\[]+)?\\]\\]", text)
            # print(anchors)
            for anchor in anchors:
                line = list(anchor)

                if line[1] != "":
                    anchor_similarity = count_anchor_similarity(line[0], line[1])
                else:
                    anchor_similarity = 1.0

                line.insert(0, title)
                line.append(anchor_similarity)

                processed_anchors.put(line)
                # print(line)


def write_to_file(OUTPUT_PATH, DISAMBIGUATION_CONTENT, r, processed_anchors):
    with open(os.path.join(OUTPUT_PATH, DISAMBIGUATION_CONTENT), "w", newline='') as disambiguation_content:
        csv_writer = csv.writer(disambiguation_content, delimiter='\t')
        finish = False

        while not (finish and processed_anchors.empty()):
            if r.poll() is True:
                finish = True

            try:
                anchors = processed_anchors.get(timeout=1)
            except Empty:
                continue

            csv_writer.writerow(anchors)


def print_status(r, articles, processed_anchors):
    finish = False

    while not finish:
        if r.poll() is True:
            finish = True

        print("Articles: For processing: {0} For writing: {1} Read: {2}".format(
            articles.qsize(),
            processed_anchors.qsize(),
            reader.status_count
        ))
        time.sleep(1)


if __name__ == "__main__":
    SRC_PATH = 'C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\data\\'
    OUTPUT_PATH = 'C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\output\\'
    SRC_FILE = 'test_sample.xml'
    DISAMBIGUATION_CONTENT = 'disambiguation_pages_content.csv'

    wiki = os.path.join(SRC_PATH, SRC_FILE)

    r, s = multiprocessing.Pipe()
    manager = multiprocessing.Manager()
    articles = manager.Queue(maxsize=1000)
    processed_articles = manager.Queue(maxsize=1000)
    processed_anchors = manager.Queue()

    reader = WikiReader(lambda ns: ns == 0, articles.put)

    status_thread = Thread(target=print_status, args=(r, articles, processed_anchors))
    status_thread.start()

    processes = []
    for i in range(1):
        processes.append(Process(target=extract_anchors, args=(r, articles, processed_anchors)))
        processes[i].start()

    write_process = Process(target=write_to_file, args=(OUTPUT_PATH, DISAMBIGUATION_CONTENT, r, processed_anchors))
    write_process.start()

    xml.sax.parse(wiki, reader)
    # signal finish
    s.send(True)

    status_thread.join()
    write_process.join()
    for process in processes:
        print("Joining thread...")
        process.join()
    print("Threads joined")
