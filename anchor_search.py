import os
import csv
import time
import xml.sax
import multiprocessing
import stemmsk
from multiprocessing.context import Process
from queue import Empty
from threading import Thread

from bs4 import BeautifulSoup
from whoosh.analysis import CharsetFilter, RegexTokenizer, LowercaseFilter, StopFilter, StemFilter
from whoosh.support.charset import accent_map

from wikireader import WikiReader
from whoosh.index import create_in
from whoosh.fields import *


def wiki_text_to_plain_text(text):
    while re.search("{{(?:[^{]|(?<!{){)*?}}", text):
        text = re.sub("{{(?:[^{]|(?<!{){)*?}}", "", text)

    # ("1. ->" + text + "<-")

    # removal of tables
    text = re.sub("\\{\\|(?s).*?\\|\\}", "", text)
    # removal of pictures
    text = re.sub("\\[\\[(Súbor|File):.*(\\[\\[(?:[^\\[]|(?<!\\[)\\[)*?\\]\\])*.*\\]\\]", "", text)
    # print("2. ->" + text + "<-")
    # removal of html
    text = re.sub("<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});", "", text)
    # cut first few sentences
    text = re.search("\\S(?:.{200,}?[^\\.]*?(?<!\\d)[\\.\n])|\\S[^\\n].*", text).group()
    # print("3. ->" + text + "<-")

    # removal of anchors with alternate text
    text = re.sub("\\[\\[[^\\[\\|]*?\\|", "", text)
    # removal of normal anchors
    text = re.sub("\\[\\[", "", text)
    text = re.sub("\\]\\]", "", text)
    text = re.sub("'{2,}", "", text)
    text = re.sub("=+ ?(.*?) ?=+", "\\1", text)

    # print("4. ->" + text + "<-")
    return text


def extract_article_info(output_path, disambiguation_content_name, receiver, articles_queue,
                         processed_disambiguation_pages_queue):
    finish = False

    while not (finish and articles_queue.empty()):
        if receiver.poll() is True:
            finish = True

        try:
            title, text = articles_queue.get(timeout=2)
        except Empty:
            continue

        if re.search("{{[Rr]ozlišovacia stránka}}", text) is None:
            with open(os.path.join(output_path, disambiguation_content_name), 'r',
                      newline='') as disambiguation_content:
                keywords = ""
                csv_reader = csv.reader(disambiguation_content, delimiter='\t')

                parsed = False
                for line in csv_reader:
                    processed_anchor = line[1]
                    processed_anchor = processed_anchor[0].upper() + processed_anchor[1:]
                    processed_anchor = BeautifulSoup(markup=processed_anchor, features="html.parser")
                    processed_anchor = str(processed_anchor)
                    processed_anchor = re.sub("^ *", "", processed_anchor)
                    processed_anchor = re.sub(" *$", "", processed_anchor)
                    processed_anchor = re.sub(" +", " ", processed_anchor)
                    # print(processed_anchor)
                    if title == processed_anchor:
                        if keywords == "":
                            keywords += line[0]
                        else:
                            keywords += " " + line[0]

                        if not parsed:
                            parsed = True
                            text = wiki_text_to_plain_text(text)

                if keywords != "":
                    processed_line = (keywords, title, text)
                    processed_disambiguation_pages_queue.put(processed_line)

                disambiguation_content.seek(0)


def write_to_file(output_path, output, receiver, processed_disambiguation_pages_queue, stopwords_list):
    with open(os.path.join(output_path, output), 'w', encoding="utf-8", newline='') as output:
        csv_writer = csv.writer(output, delimiter='\t')

        analyzer = RegexTokenizer() | LowercaseFilter() | StopFilter(stoplist=stopwords_list) | StemFilter(
            stemmsk._remove_case) | CharsetFilter(accent_map)

        schema = Schema(disambiguation_page=KEYWORD(stored=True, commas=True, analyzer=analyzer, ),
                        title=TEXT(stored=True, analyzer=analyzer, ),
                        text=TEXT(stored=True, analyzer=analyzer, ))
        ix = create_in("C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\index", schema)
        writer = ix.writer()

        finish = False

        while not (finish and processed_disambiguation_pages_queue.empty()):
            if receiver.poll() is True:
                finish = True

            try:
                processed_line = processed_disambiguation_pages_queue.get(timeout=1)
            except Empty:
                continue

            csv_writer.writerow(processed_line)

            writer.add_document(disambiguation_page=processed_line[0],
                                title=processed_line[1],
                                text=processed_line[2])

        writer.commit()


def print_status(receiver, articles_queue, processed_disambiguation_pages_queue):
    finish = False

    while not finish:
        if receiver.poll() is True:
            finish = True

        print("Articles: For processing: {0} For writing: {1} Read: {2}".format(
            articles_queue.qsize(),
            processed_disambiguation_pages_queue.qsize(),
            reader.status_count))
        time.sleep(1)


if __name__ == "__main__":
    SRC_PATH = 'C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\data\\'
    OUTPUT_PATH = 'C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\output\\'
    SRC_FILE = 'skwiki-latest-pages-articles.xml'
    DISAMBIGUATION_CONTENT = 'disambiguation_pages_content.csv'
    OUTPUT = 'output.csv'
    STOP_WORDS_FILE = 'stopwords_file.txt'

    wiki = os.path.join(SRC_PATH, SRC_FILE)

    r, s = multiprocessing.Pipe()
    manager = multiprocessing.Manager()
    articles = manager.Queue(maxsize=2000)
    processed_disambiguation_pages = manager.Queue()
    anchor_similarities = manager.Queue(maxsize=2000)

    stopwords = []

    with open(os.path.join(SRC_PATH, STOP_WORDS_FILE), "r", encoding="utf-8") as stopwords_file:
        for line in stopwords_file:
            stopwords.extend(line.split())

    reader = WikiReader(lambda ns: ns == 0, articles.put)

    status_thread = Thread(target=print_status, args=(r, articles, processed_disambiguation_pages))
    status_thread.start()

    processes = []
    for i in range(4):
        processes.append(Process(target=extract_article_info,
                                 args=(OUTPUT_PATH,
                                       DISAMBIGUATION_CONTENT,
                                       r, articles,
                                       processed_disambiguation_pages)))
        processes[i].start()

    write_process = Process(target=write_to_file,
                            args=(OUTPUT_PATH,
                                  OUTPUT, r,
                                  processed_disambiguation_pages,
                                  stopwords))
    write_process.start()

    xml.sax.parse(wiki, reader)
    # signal finish
    s.send(True)

    status_thread.join()
    print("status thread joined")
    write_process.join()
    for process in processes:
        print("Joining thread...")
        process.join()
    print("Threads joined")
