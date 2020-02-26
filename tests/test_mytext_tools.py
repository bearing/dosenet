import myText_tools as db

@Test
def test_inject():
    mydb = db.TextObject()
    data = {'hash': 'abc', 'stationID': 1, 'cpm': 2.0,
                      'cpm_error': 3.0, 'error_flag': 4}
    mydb.inject(**data)
    d = open("1_Dosimeter.txt").readlines()[-1]
    assert d.endswith(",1,2.0,3.0,4\n")