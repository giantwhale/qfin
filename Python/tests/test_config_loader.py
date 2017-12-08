from qfin.config_loader import ConfigLoader


def test_parser():
    conf = ConfigLoader()

    conf.parse_text('x = 1')
    conf.parse_text('y = 1.3')
    conf.parse_text('z = 1.2e6')
    conf.parse_text('t = [1, 2, 3]')
    conf.parse_text(' k = { "x": 9 }  # some comment ')

    assert type(conf['x']) == int
    assert conf['x'] == 1

    assert type(conf['y']) == float
    assert conf['y'] == 1.3

    assert type(conf['z']) == float
    assert conf['z'] == 1.2e6

    assert type(conf['t']) == list
    assert sum(conf['t']) == 6

    assert type(conf['k']) == dict
    assert conf['k']["x"] == 9
