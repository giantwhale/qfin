class ConfigLoader(object):

    def __init__(self, filename=None):
        self._params = {}
        if filename is not None:
            self.parse(filename)

    def get(self, name, default=None):
        return self._params.get(name, default)

    def __getitem__(self, name):
        return self._params[name]

    def items(self):
        return self._params.items()

    def parse(self, filename):
        with open(filename, 'r') as f:
            for line in f:
                self.parse_text(line)
        return self

    def parse_text(self, line):
        # This is made a separated function for easier testing
        line0, line = line, line.strip()

        pos = line.find('#')
        if pos >= 0:
            line = line[:pos].strip()

        if len(line) == 0:
            return

        pos = line.find('=')
        if pos < 0:
            raise ValueError('Failed to parse config: %s' % line0)

        name  = line[:pos].strip()
        value = _parse(line[(pos+1):])
        
        self._params[name] = value



def _parse(x):

    x = x.strip()
    if len(x) == 0:
        return None

    if _enclosed_by(x, "'") or _enclosed_by(x, '"'):
        return x[1:-1]

    if _enclosed_by(x, '()') or _enclosed_by(x, '[]') or _enclosed_by(x, '{}'):
        return eval(x)

    try:
        intval = int(x)
        return intval
    except ValueError:
        pass

    try:
        floatval = float(x)
        return floatval
    except ValueError:
        pass

    return x


def _enclosed_by(x, chars):

    if len(x) == 0:
        return False

    if len(chars) == 1:
        s, e = chars, chars
    elif len(chars) == 2:
        s, e = chars[0], chars[1]
    else:
        raise ValueError('Invalid chars: %s' % chars)

    return x[0] == s and x[-1] == e

