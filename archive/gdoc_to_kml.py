from __future__ import print_function

from pykml.factory import KML_ElementMaker as KML

import lxml.etree as etree

import lxml.html as html

import pycurl

import io

import csv

from collections import OrderedDict

from datetime import datetime

import time

import matplotlib.cm as cm

from matplotlib.colors import rgb2hex





#Planned Sampling Location

DOC1 = 'https://docs.google.com/spreadsheet/pub?key=0AgC291UE89YAdE1jRkFxTU1teFozNFdja1N0dTBvREE&single=true&output=csv&gid=0'

#Sample MetaData from Shipping Manifests

DOC2 = 'https://docs.google.com/spreadsheets/d/10X5bd77lR6w45OaNkc5A_ftZj-GhQfezKIVI4MKWrog/pubhtml'

#Sample Results

DOC3 = 'https://docs.google.com/spreadsheets/d/13xrRz21B4MjKKw99B7Of6gCrkLGD_Vohyvgi0pihZBU/pubhtml'





KML_SYMBOLS_URL = 'http://maps.google.com/mapfiles/kml'





def kmlhex(color):

    """

    Convert a matplotlib color tuple to 8-character KML hex color.

    

    Documentation:

    https://developers.google.com/kml/documentation/kmlreference#color

    

    "Color and opacity (alpha) values are expressed in hexadecimal notation.

    The range of values for any one color is 0 to 255 (00 to ff). For alpha,

    00 is fully transparent and ff is fully opaque. The order of expression

    is aabbggrr, where aa=alpha (00 to ff); bb=blue (00 to ff); gg=green

    (00 to ff); rr=red (00 to ff). For example, if you want to apply a blue

    color with 50 percent opacity to an overlay, you would specify the

    following: <color>7fff0000</color>, where alpha=0x7f, blue=0xff,

    green=0x00, and red=0x00.

    """

    # deal with RGB and RGBA

    assert(len(color) == 3 or len(color) == 4)

    hex = rgb2hex(color)

    # first character is #, so skip it

    rr = hex[1:3]

    gg = hex[3:5]

    bb = hex[5:7]

    if len(hex) == 7:

        aa = 'ff'

    else:

        aa = hex[7:9]

    return aa + bb + gg + rr





def pretty_print(doc, filename):

    # pretty print to file using an intermediate BytesIO buffer

    with io.BytesIO() as outfile:

        # write to buffer

        print(etree.tostring(doc), file=outfile)

        # read buffer back

        with io.BytesIO(outfile.getvalue()) as infile:

            # pretty print document tree to file

            with open(filename, 'w') as outfile:

                tree = etree.parse(infile)

                print(etree.tostring(tree, pretty_print=True), file=outfile)

    # read file back and replace &lt; and &gt; with < and >

    with open(filename, 'r') as infile:

        lines = infile.readlines()

    with open(filename, 'w') as outfile:

        for line in lines:

            line = line.strip('\n')

            line = line.replace('&lt;', '<')

            line = line.replace('&gt;', '>')

            line = line.replace('&amp;plusmn;', u'\u00B1')

            print(line.encode('utf-8'), file=outfile)





def google_spreadsheet(url):

    """Return list of rows (each of which is a list) for the Google doc."""

    rows = []

    buf = io.BytesIO()

    c = pycurl.Curl()

    c.setopt(c.URL, url)

    c.setopt(pycurl.SSL_VERIFYPEER, 0)

    c.setopt(pycurl.SSL_VERIFYHOST, 0)

    c.setopt(c.WRITEFUNCTION, buf.write)

    c.perform()

    with io.BytesIO(buf.getvalue()) as webfile:

        if 'output=csv' in url:

            # read old-style Google spreadsheet as a CSV file

                csvreader = csv.reader(webfile, delimiter=',')

                for j, row in enumerate(csvreader):

                    if j > 0:

                        rows.append(row)

        else:

            # read new-style Google sheet

            root = html.parse(webfile).getroot()

            tables = [table for table in root.iter('table')]

            assert(len(tables) == 1)

            table = tables[0]

            tbody = table.find('tbody')

            trs = [tr for tr in tbody.iter('tr')]

            for j, tr in enumerate(trs):

                if j >= 4:

                    row = []

                    for td in tr.iter('td'):

                        cls = td.get('class')

                        if cls is not None:

                            if cls.startswith('s'):

                                if td.text:

                                    row.append(td.text)

                                else:

                                    row.append('')

                    if row[0] != '':

                        rows.append(row)

                    else:

                        print('Null row: ', row)

            # for child in root:

            #     print(child.tag)

    # for row in rows:

    #     print(row)

    return rows





# load in and check spreadsheets

print('Loading and checking Google spreadsheets')

spreadsheet_planned_locations = google_spreadsheet(DOC1)

for row in spreadsheet_planned_locations:

    assert(len(row) == 3)

spreadsheet_sample_metadata = google_spreadsheet(DOC2)

for row in spreadsheet_sample_metadata:

    assert(len(row) == 22)

spreadsheet_sample_results = google_spreadsheet(DOC3)

for row in spreadsheet_sample_results:

    assert(len(row) == 30)

    

# build planned location dictionary

print('Sorting sample metadata and sample results into one dictionary')

dict_planned_locations = OrderedDict()

for j, row in enumerate(spreadsheet_planned_locations):

    dict_planned_locations[j] = {

        'longitude': row[0],

        'latitude': row[1],

        'description': row[2],

    }

# print(dict_planned_locations)

print('Number of planned locations: ', len(dict_planned_locations))



# The START of all sampling periods

#Need to get Period1 Start time (Just to have really)



period1 = datetime.fromtimestamp(time.mktime(time.strptime("1/01/2014 00:00","%m/%d/%Y %H:%M")))

period2 = datetime.fromtimestamp(time.mktime(time.strptime("06/01/2014 00:00","%m/%d/%Y %H:%M")))

period3 = datetime.fromtimestamp(time.mktime(time.strptime("08/01/2014 00:00","%m/%d/%Y %H:%M")))



# build the sample dictionaries

print('Sorting sample metadata and sample results into Dictionaries')



#Time Specific Results

dict_sample0 = OrderedDict() #Contains all the results

dict_sample1 = OrderedDict()

dict_sample2 = OrderedDict()

dict_sample3 = OrderedDict()



#Region Specific Results

dict_sample_longbeach = OrderedDict()

dict_sample_pnw = OrderedDict()





def dict_builder(dict_sample_info, metadata_row):

    """

    Collate the sample metadata and sample results together into a dictionary.

    metadata_row is the relevant row from the Metadata spreadsheet.

    """

    row1 = metadata_row

    sample_id = row1[0]

    dict_sample_info[sample_id] = {}

    dict_sample_info[sample_id]['sample_id'] = row1[0]

    dict_sample_info[sample_id]['collector name'] = row1[1]

    dict_sample_info[sample_id]['collector affiliation'] = row1[2]

    dict_sample_info[sample_id]['city/county/region'] = row1[3]

    dict_sample_info[sample_id]['latitude'] = row1[4]

    dict_sample_info[sample_id]['longitude'] = row1[5]

    dict_sample_info[sample_id]['collection date and time'] = \

    datetime.strptime(row1[6], '%m/%d/%Y %H:%M')

    # print(datetime.strptime(row1[6], '%m/%d/%Y %H:%M'))

    dict_sample_info[sample_id]['species'] = row1[7]

    dict_sample_info[sample_id]['status'] = "something"

    results_found = False

    for row2 in spreadsheet_sample_results:

        if row2[0] == sample_id:

            # these are the results for this sample

            results_found = True

            dict_sample_info[sample_id]['counted on'] = row2[3]

            dict_sample_info[sample_id]['delta t (days)'] = row2[4]

            dict_sample_info[sample_id]['mass (g)'] = row2[5]

            dict_sample_info[sample_id]['Livetime (days)'] = row2[6]

            dict_sample_info[sample_id]['K-40'] = row2[7]

            dict_sample_info[sample_id]['K-40 uncertainty'] = row2[8]

            dict_sample_info[sample_id]['Be-7'] = row2[9]

            dict_sample_info[sample_id]['Be-7 uncertainty'] = row2[10]

            dict_sample_info[sample_id]['Cs-137'] = row2[11]

            dict_sample_info[sample_id]['Cs-137 uncertainty'] = row2[12]

            dict_sample_info[sample_id]['Cs-134'] = row2[13]

            dict_sample_info[sample_id]['Cs-134 uncertainty'] = row2[14]

            dict_sample_info[sample_id]['U-238 series (early)'] = row2[15]

            dict_sample_info[sample_id]['U-238 series (early) uncertainty'] = row2[16]

            dict_sample_info[sample_id]['U-238 series (late)'] = row2[17]

            dict_sample_info[sample_id]['U-238 series (late) uncertainty'] = row2[18]

            dict_sample_info[sample_id]['Th-232 series (early)'] = row2[19]

            dict_sample_info[sample_id]['Th-232 series (early) uncertainty'] = row2[20]

            dict_sample_info[sample_id]['Th-232 series (late)'] = row2[21]

            dict_sample_info[sample_id]['Th-232 series (late) uncertainty'] = row2[22]

            dict_sample_info[sample_id]['I-131'] = row2[23]

            dict_sample_info[sample_id]['I-131 uncertainty'] = row2[24]

            try:

                if dict_sample_info[sample_id]['K-40'] != '':

                    dict_sample_info[sample_id]['status'] = 'Analysis complete'

                else:

                    dict_sample_info[sample_id]['status'] = 'at LBNL for analysis'

            except:

                dict_sample_info[sample_id]['status'] = 'at LBNL for analysis'

    if not results_found:

        dict_sample_info[sample_id]['status'] = 'at LBNL for analysis'

        # add placeholder data - "in progress"

        for isotope in ['K-40', 'Be-7', 'Cs-137', 'Cs-134', 'I-131',

            'U-238 series (early)', 'U-238 series (late)',

            'Th-232 series (early)', 'Th-232 series (late)']:

            dict_sample_info[sample_id][isotope] = 'in progress'

            dict_sample_info[sample_id][isotope + ' uncertainty'] = ''





# go through all the rows/samples

for row1 in spreadsheet_sample_metadata:

    sample_id = row1[0]

    timed = datetime.fromtimestamp(time.mktime(time.strptime(row1[6],"%m/%d/%Y %H:%M")))

    # print(timed < period2)

    # print(timed - period2)

    if period1 <= timed <= period2:

        print("period 1 sample added: ", sample_id)

        dict_builder(dict_sample1, row1)

        # sort period 1 dictionary by ID

        dict_sample1 = OrderedDict(sorted(dict_sample1.iteritems(), key=lambda x: x[1]['sample_id']))

        # then sort by latitude

        dict_sample1 = OrderedDict(sorted(dict_sample1.iteritems(), key=lambda x: -float(x[1]['latitude'])))

    elif period2 <= timed <= period3:

        print("period 2 sample added: ", sample_id)

        dict_builder(dict_sample2, row1)

        # sort period 2 dictionary by ID

        dict_sample2 = OrderedDict(sorted(dict_sample2.iteritems(), key=lambda x: x[1]['sample_id']))

        # then sort by latitude

        dict_sample2 = OrderedDict(sorted(dict_sample2.iteritems(), key=lambda x: -float(x[1]['latitude'])))

    elif period3 <= timed:

        print("period 3 sample added: ", sample_id)

        dict_builder(dict_sample3, row1)

        # sort period 3 dictionary by ID

        dict_sample3 = OrderedDict(sorted(dict_sample3.iteritems(), key=lambda x: x[1]['sample_id']))

        # then sort by latitude

        dict_sample3 = OrderedDict(sorted(dict_sample3.iteritems(), key=lambda x: -float(x[1]['latitude'])))

    if row1[0].startswith("LB"):

        print("LB sample added:       ", sample_id)

        dict_builder(dict_sample_longbeach, row1)

        # sort LB dictionary by ID

        dict_sample_longbeach = OrderedDict(sorted(dict_sample_longbeach.iteritems(), key=lambda x: x[1]['sample_id']))

    elif row1[0].startswith("DC-") or row1[0].startswith("RES-") \

        or row1[0].startswith("RRU-") or row1[0].startswith("WADOH") \

        or row1[0].startswith("ODFW"):

        print("PNW sample added:      ", sample_id)

        dict_builder(dict_sample_pnw, row1)

        # sort PNW dictionary by ID

        dict_sample_pnw = OrderedDict(sorted(dict_sample_pnw.iteritems(), key=lambda x: x[1]['sample_id']))

        # then sort by latitude

        dict_sample_pnw = OrderedDict(sorted(dict_sample_pnw.iteritems(), key=lambda x: -float(x[1]['latitude'])))





def number_measured(dict_samples, isotope):

    """Count how many samples in the dictionary have data for the isotope."""

    n_isot = 0

    for key in dict_samples:

        try:

            if dict_samples[key][isotope] != '' and dict_samples[key][isotope] != 'in progress':

                n_isot += 1

        except:

            pass

    return n_isot





print('')

print('Number of first period samples:              ', len(dict_sample1))

print('Number of first period samples with K-40:    ', number_measured(dict_sample1, 'K-40'))

print('Number of first period samples with Cs-137:  ', number_measured(dict_sample1, 'Cs-137'))

print('')

print('Number of second period samples:             ', len(dict_sample2))

print('Number of second period samples with K-40:   ', number_measured(dict_sample2, 'K-40'))

print('Number of second period samples with Cs-137: ', number_measured(dict_sample2, 'Cs-137'))

print('')

print('Number of third period samples:              ', len(dict_sample3))

print('Number of third period samples with K-40:    ', number_measured(dict_sample3, 'K-40'))

print('Number of third period samples with Cs-137:  ', number_measured(dict_sample3, 'Cs-137'))

print('')

print('Number of PNW samples:                       ', len(dict_sample_pnw))

print('Number of PNW samples with K-40:             ', number_measured(dict_sample_pnw, 'K-40'))

print('Number of PNW samples with Cs-137:           ', number_measured(dict_sample_pnw, 'Cs-137'))

print('')

print('Number of Long Beach samples:                ', len(dict_sample_longbeach))

print('Number of Long Beach samples with K-40:      ', number_measured(dict_sample_longbeach, 'K-40'))

print('Number of Long Beach samples with Cs-137:    ', number_measured(dict_sample_longbeach, 'Cs-137'))

print('')





dataset_planned = {

    'name': 'Planned locations',

    'stylename': 'planned-location',

    'icon': KML_SYMBOLS_URL + '/paddle/orange-blank.png',

    'data_dict': dict_planned_locations,

}



dataset_first_period = {

    'name': 'First sampling period',

    'stylename': 'first-period',

    'icon': KML_SYMBOLS_URL + '/paddle/red-blank.png',

    'data_dict': dict_sample1,

}



dataset_second_period = {

    'name': 'Second sampling period',

    'stylename': 'second-period',

    'icon': KML_SYMBOLS_URL + '/paddle/grn-blank.png',

    'data_dict': dict_sample2,

}



dataset_longbeach = {

    'name': 'Long Beach, CA',

    'stylename': 'long-beach',

    'icon': KML_SYMBOLS_URL + '/paddle/purple-blank.png',

    'data_dict': dict_sample_longbeach,

}



dataset_pnw = {

    'name': 'Pacific Northwest',

    'stylename': 'pnw',

    'icon': KML_SYMBOLS_URL + '/paddle/purple-blank.png',

    'data_dict': dict_sample_pnw,

}



# {

#     'name': 'Third sampling period',

#     'stylename': 'third-period',

#     'icon': KML_SYMBOLS_URL + '/paddle/purple-blank.png',

# },





def lat_lon_description(dataset, key):

    """Return a string to use for the description of this sample."""

    desc = ''

    lat = None

    lon = None

    if dataset['name'] == 'Planned locations':

        data_dict = dataset['data_dict'][key]

        # latitude and longitude

        lat = data_dict['latitude']

        lon = data_dict['longitude']

        # location description

        desc = '\n' \

            + '<![CDATA[\n' \

            + '<style>\n' \

            + ' table { width:300px; border:none; text-align:left; font-family:Arial, Helvetica, sans-serif; }\n' \

            + '  td { background-color:white; }\n' \

            + '</style>\n' \

            + '<table>\n' \

            + '  <tr><td><b>{}</b></td><td>{}, {}</td></tr>\n'.format(

                'Coordinates', lat, lon) \

            + '</table>\n' \

            + ']]>\n'

    else:

        data_dict = dataset['data_dict'][key]

        sample_id = data_dict['sample_id']

        name = ''

        lat = data_dict['latitude']

        lon = data_dict['longitude']

        try:

            flt_lat = float(lat)

            flt_lon = float(lon)

            # add plus signs to latitude and longitude strings

            if flt_lat > 0.:

                lat = '+' + lat

            if flt_lon > 0.:

                lon = '+' + lon

            print('{:15s}: {:12s}, {:12s}'.format(sample_id, lat, lon))

        except:

            print('Sample rejected due to bad latitude and/or longitude: '

                + '{:15s}: {:12s}, {:12s}'.format(sample_id, lat, lon))

            return None

        desc = '\n' \

            + '<![CDATA[\n' \

            + '<style>\n' \

            + ' table { border:none; border-collapse:collapse; text-align:left; font-family:Arial, Helvetica, sans-serif; }\n' \

            + '  td { background-color:white; }\n' \

            + '  th { background-color:white; }\n' \

            + '</style>\n' \

            + '<h2>Sample {}</h2>\n'.format(sample_id) \

            + '<center><table width=400px>\n' \

            + '  <tr><td><b>{}</b></td><td>{:%m/%d/%Y}</td></tr>\n'.format(

                'Collection Date', data_dict['collection date and time']) \

            + '  <tr><td><b>{}</b></td><td><i>{}</i></td></tr>\n'.format(

                'Species', data_dict['species']) \

            + '  <tr><td><b>{}</b></td><td>{}, {}</td></tr>\n'.format(

                'Coordinates', lat, lon) \

            + '  <tr><td><b>{}</b></td><td>{}</td></tr>\n'.format(

                'Collector', data_dict['collector name']) \

            + '  <tr><td><b>{}</b></td><td>{}</td></tr>\n'.format(

                'Affiliation', data_dict['collector affiliation']) \

            + '  <tr><td><b>{}</b></td><td><i>{}</i></td></tr>\n'.format(

                'Status', data_dict['status']) \

            + '</table></center>\n'

        if data_dict['status'] == 'Analysis complete':

            desc += '<center><table width=350px>\n' \

                + '  <tr><th>{}</th><th>{}</th></tr>\n'.format(

                    'Isotope', 'Result (Bq/kg dry weight)')

            isotope_list = ['K-40', 'U-238 series (early)',

                'U-238 series (late)', 'Th-232 series (early)',

                'Th-232 series (late)', 'Be-7', 'I-131', 'Cs-137', 'Cs-134']

            for isotope in isotope_list:

                if data_dict[isotope] != '':

                    if '<' in data_dict[isotope] or '&lt;' in data_dict[isotope]:

                        # below MDA, write it in gray

                        desc += '  <tr><td>{}</td><td><font color="gray">not detected ({})</font></td></tr>\n'.format(

                            isotope,

                            data_dict[isotope])

                    else:

                        desc += '  <tr><td>{}</td><td>{} &plusmn; {}</td></tr>\n'.format(

                            isotope,

                            data_dict[isotope],

                            data_dict[isotope + ' uncertainty'])

            desc += '</table></center>\n'

        desc += ']]>\n'

    return lat, lon, desc





def placemark(dataset, key):

    result = lat_lon_description(dataset, key)

    if result is None:

        return None

    lat = result[0]

    lon = result[1]

    desc = result[2]

    sample_loc = KML.Placemark(

        KML.name(''),

        KML.styleUrl('#{}'.format(dataset['stylename'])),

        KML.description(desc),

        KML.Point(

            KML.coordinates("{},{},{}".format(lon, lat, 0)),

        ),

    )

    return sample_loc





def add_dataset(doc, dataset):

    """Create placemarks for each sample in the dataset."""

    # add style for this dataset

    doc.Document.append(

        KML.Style(

            KML.IconStyle(

                KML.Icon(

                    KML.href(

                        dataset['icon']

                    )

                ),

            ),

            id=dataset['stylename'],

        )

    )

    # create a folder for this dataset

    folder = KML.Folder(

        KML.name(dataset['name']),

    )

    # add placemarks to the folder

    for key in dataset['data_dict']:

        pm = placemark(dataset, key)

        if pm is not None:

            folder.append(pm)

    doc.Document.append(folder)





def add_dataset_colorscale(dataset, isotope, name='Data',

    id='data-', cmap=cm.Reds, vmin=None, vmax=None):

    """

    Generate map placemarks that are colored according to the isotope

    data specified. cmap gives the colormap, and vmin and vmax give the minimum

    and maximum cutoffs for scaling the data.

    """

    # determine data range

    data = []

    for key in dataset['data_dict']:

        data_dict = dataset['data_dict'][key]

        try:

            datum = float(data_dict[isotope])

        except:

            continue

        data.append(datum)

    if vmin is None:

        vmin = min(data)

    if vmax is None:

        vmax = max(data)

    print('Data range: ', vmin, vmax)

    

    # generate styles for each color

    for i in range(256):

        doc.Document.append(

            KML.Style(

                KML.IconStyle(

                    KML.color(kmlhex(cmap(i))),

                    KML.Icon(

                        KML.href(

                            'http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png'

                        )

                    ),

                ),

                id=id + '{:03d}'.format(i),

            )

        )

    # add grey style for nondetects

    doc.Document.append(

        KML.Style(

            KML.IconStyle(

                KML.color(kmlhex((0.4, 0.4, 0.4))),

                KML.Icon(

                    KML.href(

                        'http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png'

                    )

                ),

            ),

            id=id + 'gray',

        )

    )

    # create a folder for these data

    folder = KML.Folder(

        KML.name(name),

    )

    # add placemarks to the folder

    for key in dataset['data_dict']:

        data_dict = dataset['data_dict'][key]

        # lat, lon, and description for this sample

        result = lat_lon_description(dataset, key)

        if result is None:

            continue

        lat = result[0]

        lon = result[1]

        desc = result[2]

        

        detect = False

        try:

            datum = data_dict[isotope]

            if datum == '':

                # print('No measurement of {:6s} for {:10s}'.format(isotope, key))

                continue

            if '&lt;' in datum or '<' in datum:

                detect = False

                # print('Measurement of {:6s} for {:10s} is a nondetect: {}'.format(

                #     isotope, key, datum))

            else:

                detect = True

                # print('Measurement of {:6s} for {:10s} is a detection: {}'.format(

                #     isotope, key, datum))

                datum = float(datum)

        except:

            # print('No measurement of {:6s} for {:10s}'.format(isotope, key))

            continue

        if detect:

            color_index = int(255. * (datum - vmin) / (vmax - vmin))

            if color_index > 255:

                color_index = 255

            if color_index < 0:

                color_index = 0

            style = '#' + id + '{:03d}'.format(color_index)

        else:

            style = '#' + id + 'gray'

        

        # add placemark to the folder

        pm = KML.Placemark(

            KML.name(''),

            KML.description(desc),

            KML.styleUrl(style),

            KML.Point(

                KML.coordinates('{},{},{}\n'.format(lon, lat, 0.)),

            ),

        )

        folder.append(pm)

    return folder





def results_table(dataset):

    """Create an HTML table from the dataset."""

    s = '\

<style>\n\

.samples-container {\n\

  width:100%;\n\

  position:relative;\n\

  display:inline-block;\n\

  border-collapse:collapse;\n\

}\n\

.samples-container table {\n\

  font-family:Arial, Helvetica, sans-serif;\n\

  float:left;\n\

  border-collapse:collapse;\n\

  table-layout:fixed;\n\

  overflow:hidden;\n\

}\n\

.samples-headercol, .samples-headercol table {\n\

  /* this must be column-sampleid + column-location */\n\

  width:240px;\n\

}\n\

.samples-table {\n\

  overflow:scroll;\n\

}\n\

.samples-table table {\n\

  /* this must be column-date + 3*column-metadata + 9*column-isotope */\n\

  width:1340px;\n\

}\n\

.samples-container .column-sampleid {\n\

  width:80px;\n\

}\n\

.samples-container .column-location {\n\

  width:160px;\n\

}\n\

.samples-container .column-date {\n\

  width:80px;\n\

}\n\

.samples-container .column-metadata {\n\

  width:90px;\n\

}\n\

.samples-container .column-isotope {\n\

  width:110px;\n\

}\n\

.samples-container td, .samples-container th {\n\

  padding-top:0.5em;\n\

  padding-bottom:0.5em;\n\

  padding-left:7px;\n\

  padding-right:7px;\n\

  text-align:center;\n\

  vertical-align:center;\n\

  overflow:hidden;\n\

}\n\

.samples-container th {\n\

  font-size:1em;\n\

  height:5em;\n\

  text-transform:none;\n\

  padding-top:5px;\n\

  padding-bottom:4px;\n\

  background-color:#13436D;\n\

  color:#ffffff;\n\

  border: 1px solid #13436D;\n\

}\n\

.samples-container th.top-row {\n\

  height:3em;\n\

}\n\

.samples-headercol tr td, .samples-table tr td {\n\

  color:#000000;\n\

  background-color:#F0F8FF;\n\

}\n\

.samples-headercol tr.alt td, .samples-table tr.alt td {\n\

  color:#000000;\n\

  background-color:#D1DEFF;\n\

}\n\

.samples-headercol tr td div, .samples-table tr td div {\n\

  font-size:0.9em;\n\

  height:3.1em;\n\

  text-align:center;\n\

  overflow:hidden;\n\

}\n\

.samples-headercol th.border-cell {\n\

  border-right: 2px solid white;\n\

}\n\

.samples-headercol td.border-cell {\n\

  border-right: 2px solid #13436D;\n\

}\n\

.samples-table th.border-cell {\n\

  border-right: 1px solid white;\n\

}\n\

.samples-table td.border-cell {\n\

  border-right: 1px solid #13436D;\n\

}\n\

</style>\n'



    # construct both the header column and the rest of the table

    s_headercol = '<div class=\"samples-headercol\">\n'

    s_headercol += '<table>'

    s_headercol += '  <tr>'

    s_headercol += '<th class="top-row column-sampleid"></th>'

    s_headercol += '<th class="top-row column-location border-cell"></th>'

    s_headercol += '</tr>\n'

    s_headercol += '  <tr>'

    # table1 is the rest of the sample metadata

    s_table = '<div class=\"samples-table\">\n'

    s_table += '<table>'

    s_table += '  <tr>'

    s_table += '<th class="top-row column-date"></th>'

    s_table += '<th class="top-row column-metadata"></th>'

    s_table += '<th class="top-row column-metadata"></th>'

    s_table += '<th class="top-row column-metadata border-cell"></th>'

    s_table += '<th colspan=3 class="top-row border-cell">Anthropogenic</th>'

    s_table += '<th colspan=6 class="top-row">Natural</th></tr>\n'

    s_table += '  <tr>'

    # write header row

    # write headers in header column

    for header in ['Sample ID', 'Location']:

        if 'Sample ID' in header:

            s_headercol += '<th class="column-sampleid">' + header + '</th>'

        elif 'Location' in header:

            s_headercol += '<th class="column-location border-cell">' + header + '</th>'

    # write headers for rest of columns

    for header in [

        'Collection Date', 'Species',

        'Latitude', 'Longitude',

        'Cs-137',

        'Cs-134',

        'I-131',

        'K-40',

        'U-238 series (early)',

        'U-238 series (late)',

        'Th-232 series (early)',

        'Th-232 series (late)',

        'Be-7']:

        if 'Date' in header:

            s_table += '<th class="column-date">' + header + '</th>'

        elif 'Species' in header or 'Latitude' in header:

            s_table += '<th class="column-metadata">' + header + '</th>'

        elif 'Longitude' in header:

            s_table += '<th class="column-metadata border-cell">' + header + '</th>'

        elif 'I-131' in header:

            s_table += '<th class="column-isotope border-cell">' + header + '</th>'

        else:

            s_table += '<th class="column-isotope">' + header + '</th>'

    s_headercol += '</tr>\n'

    s_table += '</tr>\n'



    # now write the sample rows

    # print(dataset['data_dict'].keys())

    alt = False

    for sample_id in dataset['data_dict'].keys():

        # print(sample_id)

        data_dict = dataset['data_dict'][sample_id]

        # print(data_dict)

        lat = data_dict['latitude']

        lon = data_dict['longitude']

        try:

            flt_lat = float(lat)

            flt_lon = float(lon)

            # add plus signs to latitude and longitude strings

            if flt_lat > 0.:

                lat = '+' + lat

            if flt_lon > 0.:

                lon = '+' + lon

            print('{:15s}: {:12s}, {:12s}'.format(sample_id, lat, lon))

        except:

            print('Sample rejected due to bad latitude and/or longitude: '

                + '{:15s}: {:12s}, {:12s}'.format(sample_id, lat, lon))

            continue

        if alt:

            s_headercol += '  <tr class=\"alt\">'

            s_table += '  <tr class=\"alt\">'

        else:

            s_headercol += '  <tr>'

            s_table += '  <tr>'

        alt = not alt

        s_headercol += '<td class="column-sampleid"><div>' + sample_id + '</div></td>'

        s_headercol += '<td class="column-date border-cell"><div>' + data_dict['city/county/region'] + '</div></td>'

        s_table += '<td><div>' + '{:%m/%d/%Y}'.format(

            data_dict['collection date and time']) + '</div></td>'

        s_table += '<td class="column-metadata"><div><em>' + data_dict['species'] + '</em></div></td>'

        s_table += '<td class="column-metadata"><div>' + lat + '</div></td>'

        s_table += '<td class="column-metadata border-cell"><div>' + lon + '</div></td>'

        for isotope in ['Cs-137', 'Cs-134', 'I-131', 'K-40', 'U-238 series (early)',

            'U-238 series (late)', 'Th-232 series (early)',

            'Th-232 series (late)', 'Be-7']:

            td = '<td class="column-isotope"><div>'

            if 'I-131' in isotope:

                td = '<td class="column-isotope border-cell"><div>'

            if data_dict[isotope] == '':

                # somehow this is null

                print('Warning - null data for ' + isotope)

                print(data_dict[isotope], data_dict[isotope + ' uncertainty'])

            elif 'in progress' in data_dict[isotope]:

                # result in progress

                s_table += td + '<font color="gray"><em>{}</em></font></div></td>'.format(

                    data_dict[isotope])

            elif '<' in data_dict[isotope]:

                # below MDA, write it in gray

                data_dict[isotope] = data_dict[isotope].replace('< ', '&lt;&nbsp;')

                data_dict[isotope] = data_dict[isotope].replace('<', '&lt;')

                s_table += td + '<font color="gray">nondetect ({})</font></div></td>'.format(

                    data_dict[isotope])

            else:

                s_table += td + '{} &plusmn; {}</div></td>'.format(

                    data_dict[isotope],

                    data_dict[isotope + ' uncertainty'])

        # we have finished this sample; end the table row

        s_headercol += '</tr>\n'

        s_table += '</tr>\n'

    # we have finished the entire table

    s_headercol += '</table>\n'

    s_headercol += '</div>\n'

    s_table += '</table>\n'

    s_table += '</div>\n'



    # now write both the header column and the right side of the table

    s += '<div class="samples-container">'

    s += s_headercol

    s += s_table

    s += '</div>'

    return s





def results_table_plain(dataset):

    """Create an HTML table from the dataset."""

    s = ''

    # construct the table header

    s += '<div>\n'

    s += '<table>\n'

    s += '    <thead>\n'

    # write top header row

    s += '        <tr>'

    s += '<th></th>'

    s += '<th></th>'

    s += '<th></th>'

    s += '<th></th>'

    s += '<th></th>'

    s += '<th></th>'

    s += '<th colspan=3>Anthropogenic</th>'

    s += '<th colspan=6>Natural</th></tr>\n'

    s += '        <tr>\n'

    # write header row

    for header in [

        'Sample ID', 'Location',

        'Collection Date', 'Species',

        'Latitude', 'Longitude',

        'Cs-137',

        'Cs-134',

        'I-131',

        'K-40',

        'U-238 series (early)',

        'U-238 series (late)',

        'Th-232 series (early)',

        'Th-232 series (late)',

        'Be-7']:

        s += '<th>' + header + '</th>'

    s += '</tr>\n'

    s += '    </thead>\n'



    # now write the sample rows

    s += '    <tbody>\n'

    for sample_id in dataset['data_dict'].keys():

        data_dict = dataset['data_dict'][sample_id]

        lat = data_dict['latitude']

        lon = data_dict['longitude']

        try:

            flt_lat = float(lat)

            flt_lon = float(lon)

            # add plus signs to latitude and longitude strings

            if flt_lat > 0.:

                lat = '+' + lat

            if flt_lon > 0.:

                lon = '+' + lon

            print('{:15s}: {:12s}, {:12s}'.format(sample_id, lat, lon))

        except:

            print('Sample rejected due to bad latitude and/or longitude: '

                + '{:15s}: {:12s}, {:12s}'.format(sample_id, lat, lon))

            continue

        s += '        <tr>'

        s += '<td>' + sample_id + '</td>'

        s += '<td>' + data_dict['city/county/region'] + '</td>'

        s += '<td>' + '{:%m/%d/%Y}'.format(

            data_dict['collection date and time']) + '</td>'

        s += '<td><em>' + data_dict['species'] + '</em></td>'

        s += '<td>' + lat + '</td>'

        s += '<td>' + lon + '</td>'

        for isotope in ['Cs-137', 'Cs-134', 'I-131', 'K-40',

            'U-238 series (early)', 'U-238 series (late)',

            'Th-232 series (early)', 'Th-232 series (late)',

            'Be-7']:

            s += '<td>'

            if data_dict[isotope] == '':

                # somehow this is null, should not happen

                print('Warning - null data for ' + isotope)

                print(data_dict[isotope], data_dict[isotope + ' uncertainty'])

                assert False

            elif 'in progress' in data_dict[isotope]:

                # result in progress

                s += '<font color="gray"><em>{}</em></font>'.format(

                    data_dict[isotope])

            elif '<' in data_dict[isotope]:

                # below MDA, write it in gray

                data_dict[isotope] = data_dict[isotope].replace('< ', '&lt;&nbsp;')

                data_dict[isotope] = data_dict[isotope].replace('<', '&lt;')

                s += '<font color="gray">nondetect ({})</font>'.format(

                    data_dict[isotope])

            else:

                s += '{} &plusmn; {}'.format(

                    data_dict[isotope],

                    data_dict[isotope + ' uncertainty'])

            s += '</td>'

        # we have finished this sample; end the table row

        s += '</tr>\n'

    # we have finished the entire table

    s += '    </tbody>\n'

    s += '</table>\n'

    s += '</div>\n'

    return s





# create a separate file for the planned locations

doc = KML.kml(

    KML.Document(

        KML.Name('Kelp Watch 2014'),

    )

)

add_dataset(doc, dataset_planned)

pretty_print(doc, 'kelpwatch-planned.kml')





# create another file for the samples

doc = KML.kml(

    KML.Document(

        KML.Name('Kelp Watch 2014'),

    )

)

add_dataset(doc, dataset_first_period)

add_dataset(doc, dataset_second_period)

#add_dataset(doc, dataset_third_period)

add_dataset(doc, dataset_longbeach)

add_dataset(doc, dataset_pnw)

pretty_print(doc, 'kelpwatch-results.kml')





for filename, dataset in [

        ('kelpwatch-results-1.html', dataset_first_period),

        ('kelpwatch-results-2.html', dataset_second_period),

        ('kelpwatch-results-longbeach.html', dataset_longbeach),

        ('kelpwatch-results-pnw.html', dataset_pnw),

    ]:

    with open(filename, 'w') as f:

        print(results_table(dataset), file=f)





for filename, dataset in [

        ('kelpwatch-results-plain-1.html', dataset_first_period),

        ('kelpwatch-results-plain-2.html', dataset_second_period),

        ('kelpwatch-results-plain-longbeach.html', dataset_longbeach),

        ('kelpwatch-results-plain-pnw.html', dataset_pnw),

    ]:

    with open(filename, 'w') as f:

        print(results_table_plain(dataset), file=f)





# create KML showing isotope levels in placemark colors

doc = KML.kml(

    KML.Document(

        KML.Name('Kelp Watch 2014 isotope measurements'),

    )

)

isotopes = [

    'K-40',

    'U-238 series (early)',

    'U-238 series (late)',

    'Th-232 series (early)',

    'Th-232 series (late)',

    'Be-7',

    'I-131',

    'Cs-134',

    'Cs-137'

]

datasets = {

    'First Period': dataset_first_period,

    'Second Period': dataset_second_period,

    'Pacific Northwest': dataset_pnw,

    'Long Beach': dataset_longbeach,

}

for isotope in isotopes:

    # find the maximum value for this isotope among all sets

    vmax = 0.

    for dataset in datasets:

        for sample_id in datasets[dataset]['data_dict'].keys():

            data_dict = datasets[dataset]['data_dict'][sample_id]

            if 'in progress' in data_dict[isotope]:

                pass

            elif '<' in data_dict[isotope] or '&lt;' in data_dict[isotope]:

                pass

            else:

                vmax = max([vmax, float(data_dict[isotope])])

    # create a folder for this isotope

    isotope_folder = KML.Folder(

        KML.name(isotope),

    )

    for dataset in datasets:

        identifier = dataset + '-' + isotope

        identifier = identifier.lower().replace(' ', '-')

        print(isotope, dataset, identifier)

        data_folder = add_dataset_colorscale(datasets[dataset], isotope,

            name=dataset, id=identifier, vmin=0., vmax=vmax)

        isotope_folder.append(data_folder)

    doc.Document.append(isotope_folder)



pretty_print(doc, 'kelpwatch-isotopes.kml')
