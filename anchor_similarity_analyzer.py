import csv
import os
from heapq import nsmallest

import numpy as np

if __name__ == "__main__":
    OUTPUT_PATH = 'C:\\Users\\andre\\IdeaProjects\\VINF_Disambiguation_pages\\output\\'
    DISAMBIGUATION_CONTENT = 'disambiguation_pages_content.csv'

    with open(os.path.join(OUTPUT_PATH, DISAMBIGUATION_CONTENT), "r") as disambiguation_pages_content:
        csv_reader = csv.reader(disambiguation_pages_content, delimiter='\t')

        results = []

        previous_disambig = ""
        lowest_average_score = 1
        sum = 0
        i = 0

        first = True

        for line in csv_reader:
            if first:
                first = False
                previous_disambig = line[0]

            print([line[0], line[1], line[2], line[3]])

            if line[0] == previous_disambig:
                i = i + 1
                sum += float(line[3])
            else:
                average_score = sum / i
                print(str(average_score) + " = " + str(sum) + " div " + str(i))

                # evaluate whether current is the lowest
                if average_score < lowest_average_score:
                    lowest_average_score = average_score
                    lowest_disambig = previous_disambig

                # alternative array method
                results.append((average_score, previous_disambig))

                sum = float(line[3])
                i = 1
                previous_disambig = line[0]

    n_lowest = nsmallest(10, results, key=lambda x: x[0])
    print(n_lowest)

    print(lowest_disambig + " " + str(lowest_average_score))
