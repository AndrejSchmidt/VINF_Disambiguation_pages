import csv
import os
import re
from heapq import nsmallest, nlargest

import numpy as np

if __name__ == "__main__":
    OUTPUT_PATH = 'C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\output\\'
    DISAMBIGUATION_CONTENT = 'disambiguation_pages_content.csv'

    with open(os.path.join(OUTPUT_PATH, DISAMBIGUATION_CONTENT), "r") as disambiguation_pages_content:
        csv_reader = csv.reader(disambiguation_pages_content, delimiter='\t')

        number_of_disambig_pages = 0

        results = []
        brackets_dict = {}

        previous_disambig = ""
        lowest_average_score = 1
        sum = 0
        i = 0

        first = True

        for line in csv_reader:
            if first:
                first = False
                previous_disambig = line[0]
                number_of_disambig_pages += 1

            p = re.compile(".*\\((.*)\\).*")

            m = p.match(line[1])
            if m:
                print(m.group(1))
                # print([line[0], line[1], line[2], line[3]])
                if m.group(1) in brackets_dict:
                    brackets_dict[m.group(1)] += 1
                else:
                    brackets_dict[m.group(1)] = 1

            if line[0] == previous_disambig:
                i = i + 1
                sum += float(line[3])
            else:
                number_of_disambig_pages += 1
                average_score = sum / i
                # print(str(average_score) + " = " + str(sum) + " div " + str(i))

                # evaluate whether current is the lowest
                if average_score < lowest_average_score:
                    lowest_average_score = average_score
                    lowest_disambig = previous_disambig

                # alternative array method
                results.append((average_score, previous_disambig))

                sum = float(line[3])
                i = 1
                previous_disambig = line[0]

            # last element

    # printout of least diff ancgor texts
    n_lowest = nsmallest(10, results, key=lambda x: x[0])
    print(n_lowest)

    print(lowest_disambig + " " + str(lowest_average_score))

    # printout of topics
    n_largest = nlargest(20, brackets_dict.items(), key=lambda x: x[1])
    print(n_largest)

    print("number of disambiguation pages: " + str(number_of_disambig_pages))
