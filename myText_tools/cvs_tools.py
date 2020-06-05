import re
from os import listdir
from os.path import isfile, join
import numpy as np

"""Create a script that takes all files: fix the d3s with """

def fixCsvColumns(inputfile, outputFile):
    inf = open(inputfile, "r")
    ouf = open(outputFile, "w")
    header = inf.readline().strip()
    header = header.split(",")
    if header[-1] != "error_flag":
        header.append("error_flag")
    print(",".join(header), file=ouf)
    lines = inf.readlines()
    ouf.writelines(lines)
    inf.close()
    ouf.close()

def readLine(inFile, minComma):
    buffer = ""
    commas = 0
    while True:
        ch = inFile.read(1)
        if ch == "":
            break
        if ch == ",":
            commas += 1
        elif commas > minComma and ch == "\n":
            break
        buffer += ch
    return buffer

def readLine(inFile):
    buffer = b""
    normal_in = 0
    while True:
        ch = inFile.read(1)
        buffer += ch
        if ch == b"0":
            normal_in += 1
        if ch == b"":
            break
        if ch == b"\n" and normal_in > 5:
            break
    return buffer

def convert_byte(line):
    binary_yet = False
    buffer = ""
    binary_data_list = []
    for ch in line:
        if ch < 32 and not ch == 10:
            binary_yet = True
        if not binary_yet:
            buffer += chr(ch)
        else:
            binary_data_list.append(ch)
    binary_data_list = binary_data_list[:-3]
    while len(binary_data_list) < 4096:
        binary_data_list.insert(0, 0)
        print(len(binary_data_list))
    spectrum = np.array(binary_data_list)
    rebin_array = spectrum.reshape(len(spectrum) // 4, 4).sum(1)
    spectrum_string = ",".join(map(str, rebin_array.tolist()))
    split_buffer = buffer.split(",")
    cpm = int(split_buffer[-2]) / 5
    cpm_error = np.sqrt(int(split_buffer[-2])) / 5
    split_buffer[-2] = str(cpm)
    buffer = ",".join(split_buffer)
    keV_per_ch = 2.57
    return buffer + str(cpm_error) + "," + str(keV_per_ch) + "," + spectrum_string + ",0" + "\n"

def d3s_files_converter(Data_Path = "/Users/ethanchang/dosenet/dosenet_data/dosenet/dosenet_data/testing_data", output_dir = "/Users/ethanchang/dosenet/myText_tools/tmp/dosenet"):
    """convert all d3s files in one directory to readable d3s files"""
    files = [f for f in listdir(Data_Path) if isfile(join(Data_Path, f)) and re.fullmatch('.+_d3s\.csv', f)]
    for f in files:
        """for each file that matches the regex expression, read the entire file as panda dataframe, 
        then put all the data frame to each correct column. Last convert the spectrum column
        to all spectrum string. Last, overwrite the original d3s file"""
        d3sFile = open(join(Data_Path, f), "rb")
        """"how to read each data point, rather than just line by line?"""
        writeTo = open(join(output_dir, f), "w")
        line = b""
        binary_yet = False
        ten_ind = 0
        four_ind = 0
        eight_ind = 0
        counter = 0
        while True:
            ch = d3sFile.read(1)
            if ch == b"":
                break
            line += ch
            if ord(ch) < 32 and ord(ch) != 10:
                binary_yet = True
            if ord(ch) == 44:
                four_ind = counter
            if ord(ch) == 48:
                eight_ind = counter
            if ord(ch) == 10:
                ten_ind = counter
            if binary_yet and four_ind + 2 == ten_ind and four_ind + 1 == eight_ind and eight_ind + 1 == ten_ind:
                converted_line = convert_byte(line)
                print(converted_line, file=writeTo, end='')
                line = b""
                ten_ind = 0
                four_ind = 0
                eight_ind = 0
            elif not binary_yet and ten_ind != 0:
                print(line.decode("ascii"), file=writeTo, end='')
                line = b""
                ten_ind = 0
                four_ind = 0
                eight_ind = 0
            counter += 1
        writeTo.close()
        d3sFile.close()

def convert_binary(text):
    data = text.split(",")
    for i in range(len(data)):
        if contains_binary(data[i]):
            spectrum = np.fromstring(data[i], dtype="uint8")
            rebin_array = spectrum
            spectrum_string = ", ".join(map(str, rebin_array.tolist()))
            data[i] = spectrum_string
    return ",".join(data)

def fix_all(data_path = "/Users/ethanchang/dosenet/dosenet_data/dosenet/dosenet_data/testing_data", output_dir = "/Users/ethanchang/dosenet/myText_tools/tmp/dosenet"):
    files = [f for f in listdir(data_path) if isfile(join(data_path, f)) and re.fullmatch('^((?!(year|month|week|day|hour)).)*$', f)]
    for f in files:
        fixCsvColumns(join(data_path, f), join(output_dir, f))

def contains_binary(text):
    for ch in text:
        if ord(ch) < 32:
            return True
    return False

def contains_binary_bi(text):
    fortyFour = False
    fortyEight = False
    ten = False
    for ch in text:
        if fortyFour and fortyEight and ch == 10:
            ten = True
        elif fortyFour and fortyEight:
            fortyFour = False
            fortyEight = False
        if fortyFour and ch == 48:
            fortyEight = True
        elif fortyFour:
            fortyFour = False
        if ch == 44:
            fortyFour = True
        if fortyEight and fortyFour and ten:
            return True
    return False

def trim_file(Data_Path = "/Users/ethanchang/dosenet/dosenet_data/dosenet/dosenet_data/testing_data", output_dir = "/Users/ethanchang/dosenet/myText_tools/tmp/dosenet"):
    files = [f for f in listdir(Data_Path) if isfile(join(Data_Path, f)) and re.fullmatch('.+_d3s\.csv', f)]
    for f in files:
        d3sFile = open(join(Data_Path, f), "r")
        """"how to read each data point, rather than just line by line?"""
        writeTo = open(join(output_dir, f), "w")
        lines = d3sFile.readlines()
        length = len(lines)
        removeUp = 1000
        removeDown = length - 2000
        for lineInd in range(0, length):
            if lineInd < removeUp or lineInd > removeDown:
                print(lines[lineInd], file=writeTo)

def convert_d3s(Data_Path = "/Users/ethanchang/dosenet/dosenet_data/dosenet/dosenet_data/testing_data", output_dir = "/Users/ethanchang/dosenet/myText_tools/tmp/dosenet"):
    fileName = "singleD3sString_d3s.csv"
    dataCounter = 0
    d3sFile = open(join(Data_Path, fileName), "rb")
    for line in d3sFile.readlines():
        print(line)
        print(np.frombuffer(line, dtype="uint8"))
        dataCounter += len(np.frombuffer(line, dtype="uint8"))
    """"how to read each data point, rather than just line by line?"""
    print(dataCounter)
    d3sFile = open(join(Data_Path, fileName), "rb")
    writeTo = open(join(output_dir, fileName), "w")
    buffer = b""
    while True:
        ch = d3sFile.read(1)
        if ch == b"":
            break
        buffer += ch
    barray = bytearray(buffer)
    spectrum = np.frombuffer(buffer, dtype="uint8")
    if len(spectrum) < 4096:
        diff = 4096 - len(spectrum)
        print(diff)
        zs = np.zeros(diff)
        print(zs)
        spectrum = np.concatenate((zs, spectrum))
    rebin_array = spectrum
    spectrum_string = ", ".join(map(str, rebin_array.tolist()))
    print(barray)
    print(len(barray))
    print(spectrum_string, file=writeTo)
    print(buffer)
    print(spectrum_string)
    data = spectrum_string.split(",")
    print(len(data))

def find_binary(data_path = "/Users/ethanchang/dosenet/dosenet_data/dosenet/dosenet_data/testing_data"):
    files = [f for f in listdir(data_path) if
             isfile(join(data_path, f)) and re.fullmatch('^((?!(year|month|week|day|hour)).)*$', f)]
    for f in files:
        file = open(join(data_path, f), 'rb')
        while True:
            ch = file.read(1)
            if ch == b"":
                break
            if ord(ch) < 32 and ord(ch) != 10:
                print(f)
                break

#fixCsvColumns("/Users/ethanchang/dosenet/dosenet_data/dosenet/uw_adc.csv", "/Users/ethanchang/dosenet/dosenet_data/dosenet/uw_adc-f2.csv")
#fix_all()
#d3s_files_converter()
# trim_file()
#convert_d3s()
find_binary()