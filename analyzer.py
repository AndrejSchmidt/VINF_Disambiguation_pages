from whoosh.index import open_dir

ix = open_dir("index")
with ix.searcher() as searcher:
    print("This is analyzer:")

    reader = ix.reader()
    most_frequent_terms = reader.most_frequent_terms("text", number=10, prefix='')
    most_distinctive_terms = reader.most_distinctive_terms("text", number=10, prefix='')

    print(most_frequent_terms)
    print(most_distinctive_terms)
