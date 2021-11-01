import logging
import os
from datetime import datetime
from pathlib import Path

import pyodbc  # type: ignore
from typing import List, Optional, Union, Generator

from seiso.console.helpers import log_path
from seiso.services.promus.authorities import AuthorityCollections, ItemCollection

logger = logging.getLogger(__name__)
update_logger = logging.getLogger('promus_update_logger')

ColumnDataTypes = List[Union[str, int, None]]


class MsSql:

    def __init__(self, update_log: Optional[Path] = None, read_only_mode=True, **db_settings):
        if os.name == 'posix':
            connection_args = [
                'DRIVER={FreeTDS}',
                'Server=%(server)s',
                'Database=%(database)s',
                'UID=%(user)s',
                'PWD=%(password)s',
                'TDS_Version=8.0',
                'Port=%(port)s',
            ]
        else:
            connection_args = [
                'Driver={ODBC Driver 17 for SQL Server}',
                'Server=tcp:%(server)s,%(port)s',
                'Database=%(database)s',
                'UID=%(user)s',
                'PWD=%(password)s',
            ]
        connection_string = ';'.join(connection_args) % db_settings
        self.connection: pyodbc.Connection = pyodbc.connect(connection_string)
        self.update_log = update_log
        self.read_only_mode = read_only_mode

    def cursor(self) -> pyodbc.Cursor:
        return self.connection.cursor()

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def select(self, query: str, params: ColumnDataTypes = None, normalize: bool = False,
               date_fields: list = None) -> Generator[dict, None, None]:
        if 'SELECT' not in query:
            raise Exception('Not a SELECT query')
        with self.cursor() as cursor:
            # print(query)
            cursor.execute(query, params or [])
            columns = [column[0] for column in cursor.description]
            for row in cursor.fetchall():
                row = dict(zip(columns, row))
                if normalize:
                    self.normalize_row(row, date_fields=date_fields)
                yield row

    def update(self, query: str, params: ColumnDataTypes) -> int:
        log_entry = self.format_log_entry(query, params)
        logger.debug(f"Query: {log_entry}")
        # if self.update_log is not None:
        #     with self.update_log.open('a+') as fp:
        #         fp.write(log_entry + '\n')
        with self.cursor() as cursor:
            cursor.execute(query, params)
            rowcount = cursor.rowcount
            if self.read_only_mode:
                logger.info('Read only mode, so rolling back query. Rows that would have been affected: %d', rowcount)
                self.rollback()
            else:
                update_logger.info('Executed query: %s - Affected rows: %d', log_entry, rowcount)
                self.commit()
        return rowcount

    @staticmethod
    def normalize_row(row: dict, date_fields: List[str] = None) -> None:
        """
        In-place normalization of a row:

        - Ensures values are either strings or dates
        - Empty strings are converted to NULLs
        - Numbers are converted to strings
        - All strings are trimmed
        """
        date_fields = date_fields or []
        for k in row.keys():
            if row[k] is None:
                continue
            elif k in date_fields:
                row[k] = row[k].date() if row[k] else None
            else:
                row[k] = str(row[k]).strip()
                if row[k] == '':
                    row[k] = None

    @staticmethod
    def format_log_entry(query: str, params: ColumnDataTypes, date_prefix=True) -> str:
        ret = '%s params=(%s)' % (query, ', '.join([
            repr(param) for param in params
        ]))
        if date_prefix:
            ret = '[%s] %s' % (datetime.now().isoformat(), ret)
        return ret

    def close(self):
        self.connection.close()


class Promus:

    def __init__(self, server=None, port=None, database=None, user=None, password=None, update_log: Optional[Path] = None, read_only_mode: bool = True):
        if update_log is None:
            update_log = log_path('promus_updates.log')
        self.connection_options = {
            'server': server or os.getenv('PROMUS_HOST'),
            'port': port or os.getenv('PROMUS_PORT'),
            'database': database or os.getenv('PROMUS_DATABASE'),
            'user': user or os.getenv('PROMUS_USER'),
            'password': password or os.getenv('PROMUS_PASSWORD'),
            'update_log': update_log,
            'read_only_mode': read_only_mode,
        }
        self.connection_options['update_log'].parent.mkdir(exist_ok=True, parents=True)
        self.connection_options['update_log'].touch()

        self.authorities = AuthorityCollections(self)
        self.items = ItemCollection(self)

    def connection(self) -> MsSql:
        # Seems like only one cursor can be opened per connection.
        # Therefore, we sometimes need to open more than one connection.
        return MsSql(**self.connection_options)

