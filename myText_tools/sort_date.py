import re
from os import listdir
from os.path import isfile, join
import csv
import operator

# read in all the csv in a directory and write a function that sort the csv with date
# sort data function will be given an input file and an output file => sorted output into input

def sort_all(data_path = "/home/dosenet/backups/tmp/dosenet", output_dir = "/home/dosenet/backups/tmp1/dosenet"):
    files = [f for f in listdir(data_path) if isfile(join(data_path, f)) and re.fullmatch('^((?!(year|month|week|day|hour)).)*$', f)]
    for f in files:
        sort_csv(join(data_path, f), join(output_dir, f))

def sort_csv(inputfile, outputFile):
    inf = open(inputfile, "r")
    ouf = open(outputFile, "w")
    all = []
    reader = csv.reader(inf, delimiter=",")
    # print(all)
    for item in reader:
        all.append(item)
        # print(item)
    lines = all[1:]
    header = all[0]
    # sort by unix time
    if len(lines) > 0:
        sorted_lines = sorted(lines, key=operator.itemgetter(2), reverse=False)
    print(",".join(header), file=ouf)
    for line in sorted_lines:
        print(",".join(line), file=ouf)
    inf.close()
    ouf.close()

sort_all()
