import re
from os import listdir
from os.path import isfile, join
import csv
import operator

# read in all the csv in a directory and write a function that sort the csv with date
# sort data function will be given an input file and an output file => sorted output into input

def sort_all(data_path = "/home/dosenet/backups/tmp/dosenet", output_dir = "/home/dosenet/backups/tmp_test/dosenet"):
    files = [f for f in listdir(data_path) if isfile(join(data_path, f)) and re.fullmatch('^((?!(year|month|week|day|hour)).)*$', f)]
    for f in files:
        sort_csv(join(data_path, f), join(output_dir, f))

def sort_csv(inputfile, outputFile):
    inf = open(inputfile, "r")
    ouf = open(outputFile, "w")
    all = []

    # clean out any null data first
    data_initial = open(inputfile, "rb")
    reader = csv.reader((line.replace('\0','') for line in data_initial), delimiter=",")
    #reader = csv.reader(inf, delimiter=",")

    # print(all)
    try:
        for item in reader:
            all.append(item)
            # print(item)
        lines = all[1:]
        header = all[0]
        print(",".join(header), file=ouf)
        # sort by unix time
        if len(lines) > 0:
            try:
                sorted_lines = sorted(lines, key=operator.itemgetter(2), reverse=False)
                print("Writing sorted lines to new file: {}".format(ouf))
                for line in sorted_lines:
                    print(",".join(line), file=ouf)
            except Exception as e:
                print("Error:")
                print(e)
                print(lines)
    except Exception as e:
        print(e)
        print(inputfile)
    inf.close()
    ouf.close()

sort_all()
