DATE_FORMAT = '%Y-%m-%d'


def ids_to_str(ids):
    s = ''
    for i in ids:
        s += '%s,' % i
    return s[:-1]
