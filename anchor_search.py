import os
import re
import csv
import time
import xml.sax
import multiprocessing
from queue import Empty
from threading import Thread
from wikireader import WikiReader
from whoosh.index import create_in
from whoosh.fields import *


def wiki_text_to_plain_text(text):
    while re.search("{{(?:[^{]|(?<!{){)*?}}", text):
        text = re.sub("{{(?:[^{]|(?<!{){)*?}}", "", text)

        # ("1. ->" + text + "<-")

    # removal of pictures
    text = re.sub("\\[\\[(Súbor|File):.*(\\[\\[(?:[^\\[]|(?<!\\[)\\[)*?\\]\\])*.*\\]\\]", "", text)
    # print("2. ->" + text + "<-")
    # cut first few sentences
    text = re.search("\\S(?:[^\\.]*?[\\.\n]){1}|\\S[^\\n].*", text).group()
    # print("3. ->" + text + "<-")

    # removal of anchors with alternate text
    text = re.sub("\\[\\[[^\\[\\|]*?\\|", "", text)
    # removal of normal anchors
    text = re.sub("\\[\\[", "", text)
    text = re.sub("\\]\\]", "", text)
    text = re.sub("'''", "", text)
    text = re.sub("''", "", text)
    text = re.sub("'''''", "", text)

    # print("4. ->" + text + "<-")
    return text


def extract_article_info():
    while not (finish and articles.empty()):
        try:
            title, text = articles.get(timeout=2)
        except Empty:
            continue

        with open(os.path.join(OUTPUT_PATH, DISAMBIGUATION_CONTENT), 'r', newline='') as disambiguation_content:
            keywords = ""
            csv_reader = csv.reader(disambiguation_content, delimiter='\t')

            parsed = False
            for line in csv_reader:
                if title == line[1]:
                    if keywords == "":
                        keywords += line[0]
                    else:
                        keywords += " " + line[0]

                    if not parsed:
                        parsed = True
                        text = wiki_text_to_plain_text(text)

            if keywords != "":
                processed_line = (keywords, title, line[2], text)
                processed_disambiguation_pages.put(processed_line)

            disambiguation_content.seek(0)


def write_to_file():
    with open(os.path.join(OUTPUT_PATH, OUTPUT), 'w', newline='') as output:
        csv_writer = csv.writer(output, delimiter='\t')

        schema = Schema(disambiguation_page=KEYWORD(stored=True, commas=True),
                        title=TEXT(stored=True),
                        anchor_text=STORED,
                        text=TEXT(stored=True))
        ix = create_in("C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\index", schema)
        writer = ix.writer()

        while not (finish and processed_disambiguation_pages.empty()):
            try:
                processed_line = processed_disambiguation_pages.get(timeout=1)
            except Empty:
                continue

            csv_writer.writerow(processed_line)

            writer.add_document(disambiguation_page=processed_line[0],
                                title=processed_line[1],
                                anchor_text=processed_line[2],
                                text=processed_line[3])

        writer.commit()


def print_status():
    while not finish:
        print("Articles: For processing: {0} For writing: {1} Read: {2}".format(
            articles.qsize(),
            processed_disambiguation_pages.qsize(),
            reader.status_count))
        time.sleep(1)


if __name__ == "__main__":
    finish = False
    extracting_done = False
    SRC_PATH = 'C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\data\\'
    OUTPUT_PATH = 'C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\output\\'
    SRC_FILE = 'test_sample.xml'
    DISAMBIGUATION_CONTENT = 'disambiguation_pages_content.csv'
    OUTPUT = 'output.csv'

    wiki = os.path.join(SRC_PATH, SRC_FILE)

    manager = multiprocessing.Manager()
    articles = manager.Queue(maxsize=1000)
    processed_articles = manager.Queue(maxsize=1000)
    processed_disambiguation_pages = manager.Queue()

    reader = WikiReader(lambda ns: ns == 0, articles.put)

    status_thread = Thread(target=print_status, args=())
    status_thread.start()

    threads = []
    for _ in range(1):
        threads.append(Thread(target=extract_article_info))
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
