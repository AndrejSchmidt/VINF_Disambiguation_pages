from whoosh.index import open_dir

ix = open_dir("index")
with ix.searcher() as searcher:
    print("This is analyzer:")

    reader = ix.reader()
    most_frequent_terms = reader.most_frequent_terms("text", number=20, prefix='')
    most_distinctive_terms = reader.most_distinctive_terms("text", number=20, prefix='')
    doc_count = reader.doc_count_all()

    print(most_frequent_terms)
    print(most_distinctive_terms)
    print(doc_count)
