import psycopg2
import psycopg2.extras

class DB(object):
    def __init__(self, host, dbname, port=5432, user=None, password=None,
                 autocommit=True, schema='public'):

        self.conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port
        )
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.schema = schema
        self.autocommit = autocommit
        self.conn.autocommit = autocommit

    def commit(self):
        self.conn.commit()

    def close(self):
        if self.conn is not None:
            self.conn.close()

    def json(self, value):
        return psycopg2.extras.Json(value)

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self.cur.execute(f'set search_path={value}')
        self.conn.commit()
        self._schema = value

    def fetchone(self):
        return self.cur.fetchone()

    def fetchall(self):
        return self.cur.fetchall()

    def fetchmany(self, size=-1):
        return self.cur.fetchmany(size)

    def execute(self, sql, params=None):
        try:
            self.cur.execute(sql, params)
        except Exception as e:
            print(e)
            print(e.message)
            self.conn = psycopg2.connect(
                host=host,
                dbname=dbname,
                user=user,
                password=password,
                port=port
            )
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


    def insert(self, tbl, data):
        if isinstance(data[0], list):
            for row in data:
                sql = f'insert into "{tbl}" values (' + ','.join(["%s"] * len(row)) + ')'
                self.cur.execute(sql, row)
        elif isinstance(data[0], dict):
            for row in data:
                sql = f'insert into "{tbl}" (' + ','.join([f'"{x}"' for x in sorted(row)]) + ') values (' + ','.join(
                    ["%s"] * len(row)) + ')'
                self.cur.execute(sql, [row[x] for x in sorted(row)])
        self.commit()

    def insert_returning_id(self, tbl, data):
        if isinstance(data[0], list):
            for row in data:
                sql = f'insert into "{tbl}" values (' + ','.join(["%s"] * len(row)) + ') returning id'
                self.cur.execute(sql, row)
        elif isinstance(data[0], dict):
            for row in data:
                sql = f'insert into "{tbl}" (' + ','.join([f'"{x}"' for x in sorted(row)]) + ') values (' + ','.join(
                    ["%s"] * len(row)) + ') returning id'
                self.cur.execute(sql, [row[x] for x in sorted(row)])
        self.commit()
        return self.cur.fetchone()[0]


    def delete(self, tbl, where=None):
        params = []
        sql = f'delete from "{tbl}"'
        if where:
            sql += ' where %s' % where
        self.cur.execute(sql)
