from whoosh.qparser import MultifieldParser
from whoosh.index import open_dir

ix = open_dir("index")
with ix.searcher() as searcher:
    print("Type your search query (-exit for exit):")

    while True:
        user_input = str(input())

        if user_input == "-exit":
            break

        query = MultifieldParser(["title", "disambiguation_page", "text"], ix.schema).parse(user_input)
        results = searcher.search(query, limit=None)

        for result in results:
            result_list = result.values()
            # print(result.values())
            print("Title: {0}\nDisambiguation pages: {1}\nText: {2}\n".format(
                result_list[2],
                result_list[0],
                result_list[1]))

        print("Number of results: " + str(len(results)))
