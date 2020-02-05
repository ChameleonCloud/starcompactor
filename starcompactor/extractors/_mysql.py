# coding: utf-8
# MySQL helpers lifted from https://github.com/ChameleonCloud/hammers
import itertools
import codecs
import glob
import logging
import os
import stat
import configparser

KEYERROR_LIKE_OPTIONERRORS = (
    configparser.NoSectionError,
    configparser.NoOptionError,
)

# https://dev.mysql.com/doc/refman/5.7/en/option-files.html
# https://mariadb.com/kb/en/mariadb/configuring-mariadb-with-mycnf/
MYCNF_PATHS = [
    '/etc/my.cnf',
    '/etc/mysql/my.cnf',
#     'SYSCONFDIR/my.cnf',
#     'defaults-extra-file' # The file specified with --defaults-extra-file, if any
    '~/.my.cnf',
    '~/.mylogin.cnf',
]

__all__ = ['MyCnf', 'MYCNF_PATHS', 'MySqlArgs', 'MySqlShim']


class MyCnf(object):
    L = logging.getLogger('.'.join([__name__, 'MyCnf']))

    def __init__(self, paths=None):
        if paths is None:
            paths = MYCNF_PATHS

        self.path_stack = list(reversed(paths))
        self.cp = configparser.ConfigParser(allow_no_value=True)

        self.load()

    def valid_path(self, path):
        self.L.debug('checking path {} for validity'.format(path))
        try:
            cnf_stat = os.stat(path)
        except (IOError, OSError):
            self.L.debug('failed to stat path {} (can\'t read/doesn\'t exist?)'.format(path))
            return False

        if cnf_stat.st_mode & stat.S_IWOTH:
            self.L.debug('path {} is world-writable, ignoring'.format(path))
            return False

        return True

    def read(self, path):
        with codecs.open(path) as f:
            for line in f:
                if line.startswith('!'):
                    self.L.debug('found magic directive, line: "{}"'.format(line))

                    self.magic(path, line)
                else:
                    yield line

    def magic(self, sourcefile, line):
        directive, args = line.split(None, 1)
        directive = directive.lstrip('!')
        return {
            'include': self.include,
            'includedir': self.includedir,
        }[directive](sourcefile, args)

    def include(self, source_file, include):
        new_path = os.path.join(os.path.dirname(source_file), include)
        self.L.debug('adding path from source file "{}": {}'.format(
                source_file, new_path))
        self.path_stack.append(new_path)

    def includedir(self, source_file, includedir):
        new_paths = list(glob.iglob(os.path.join(os.path.dirname(source_file), includedir, '*.cnf')))
        self.L.debug('adding paths from source file "{}" found in dir "{}": {}'.format(
                source_file, includedir, new_paths))
        self.path_stack.extend(new_paths)

    def load(self):
        while self.path_stack:
            path = self.path_stack.pop()
            self.L.debug('processing possible path "{}"'.format(path))

            path = os.path.expanduser(path)

            if not self.valid_path(path):
                continue

            self.L.debug('loading/merging file "{}"'.format(path))
            self.read_file(path)

    def read_file(self, path):
        self.cp.read_file(self.read(path))

    def __iter__(self):
        return iter(self.cp)

    def __getitem__(self, key):
        try:
            d = dict(self.cp[key])
        except KEYERROR_LIKE_OPTIONERRORS as e:
            raise KeyError(str(e))

        return d


class MySqlArgs(object):
    def __init__(self, defaults, mycnfpaths=None):
        mycnf = MyCnf(mycnfpaths)

        for client_key in ['user', 'password', 'host', 'port']:
            try:
                new_value = mycnf['client'][client_key]
            except KeyError:
                continue
            defaults[client_key] = new_value

        self.defaults = defaults

    def inject(self, parser):
        parser.add_argument('-u', '--db-user', type=str,
            default=self.defaults['user'],
            help='Database user (defaulting to "%(default)s")',
        )
        parser.add_argument('-p', '--password', type=str,
            default=self.defaults['password'],
            help='Database password (default empty or as configured with .my.cnf)',
        )
        parser.add_argument('-H', '--host', type=str,
            default=self.defaults['host'],
            help='Database host (defaulting to "%(default)s")',
        )
        parser.add_argument('-P', '--port', type=int,
            default=int(self.defaults['port']),
            help='Database port, ignored for local connections as the UNIX socket '
                 'is used. (defaulting to "%(default)s")',
        )

    def extract(self, args):
        pwd = args.password
        if pwd and len(pwd) > 1 and pwd[0] == pwd[-1] and pwd[0] in '\'\"':
            pwd = pwd[1:-1]

        self.connect_kwargs = {
            'user': args.db_user,
            'passwd': pwd,
            'host': args.host,
            'port': args.port,
        }

    def connect(self, **overrides):
        connect_kwargs = self.connect_kwargs.copy()
        connect_kwargs.update(overrides)
        return MySqlShim(**connect_kwargs)


class MySqlShim(object):
    batch_size = 100
    limit = 1000

    def __init__(self, **connect_args):
        # lazy load so to avoid installing the Python
        # package which also requires the MySQL headers...
        import MySQLdb

        self.db = MySQLdb.connect(**connect_args)
        self.cursor = self.db.cursor()

    def columns(self):
        return [cd[0] for cd in self.cursor.description]

    def query(self, *cargs, **ckwargs):
        limit = ckwargs.pop('limit', self.limit)

        if ckwargs.pop('immediate', False):
            return list(itertools.islice(self._query(*cargs, **ckwargs), limit))
        else:
            return itertools.islice(self._query(*cargs, **ckwargs), limit)

    def _query(self, *cargs, **ckwargs):
        self.cursor.execute(*cargs, **ckwargs)
        fields = self.columns()
        rows = self.cursor.fetchmany(self.batch_size)
        while rows:
            for row in rows:
                yield dict(zip(fields, row))
            rows = self.cursor.fetchmany(self.batch_size)


if __name__ == '__main__':
    import json
    logging.basicConfig(level=logging.DEBUG)
    mycnf = MyCnf(MYCNF_PATHS)
    print(json.dumps({sec: mycnf[sec] for sec in mycnf}, indent=4))
