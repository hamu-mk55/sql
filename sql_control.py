import sqlite3
import pandas as pd


class SQLApp:
    def __init__(self, column_dict: dict, db_name: str = 'test.db', table_name: str = 'test'):
        self.__db_name: str = db_name
        self.__table_name: str = table_name

        self.is_opened: bool = False

        self.conn: sqlite3.Connection | None = None
        self.cur: sqlite3.Cursor | None = None
        self.column_dict: dict | None = column_dict

    def __del__(self):
        self.close_db_table()

    def open_db_table(self) -> None:
        if self.is_opened: return

        self.conn = sqlite3.connect(self.__db_name,
                                    check_same_thread=False)
        self.cur = self.conn.cursor()

        self.cur.execute(f"SELECT * FROM sqlite_master "
                         f"WHERE type='table' and name='{self.__table_name}'")

        if not self.cur.fetchone():
            self.make_table()

        self.is_opened = True

    def make_table(self) -> None:
        SQL = f'CREATE TABLE {self.__table_name}(' \
              f'table_id INTEGER PRIMARY KEY AUTOINCREMENT,'

        for key, val in self.column_dict.items():
            SQL += f'{key} {val},'

        SQL = SQL[:-1]
        SQL += f')'

        self.cur.execute(SQL)
        self.conn.commit()

    def close_db_table(self) -> None:
        if not self.is_opened: return

        if self.cur is not None:
            self.cur.close()

        if self.conn is not None:
            self.conn.close()

        self.is_opened = False

    def insert_data(self, **kwargs) -> None:
        if len(kwargs) == 0: return

        keys_str = ''
        vals_str = ''
        vals_list = []
        for key, val in kwargs.items():
            if key in self.column_dict:
                keys_str += f'{key},'
                vals_str += '?,'
                vals_list.append(val)
        keys_str = keys_str[:-1]
        vals_str = vals_str[:-1]

        _SQL = f"INSERT INTO {self.__table_name}({keys_str}) values({vals_str})"

        self.cur.execute(_SQL, tuple(vals_list))

        self.conn.commit()

    def delete_data(self, table_id: int) -> None:
        self.cur.execute(f'DELETE FROM {self.__table_name} WHERE table_id = ?',
                         (table_id,))
        self.conn.commit()

    def update_data(self, table_id: int, **kwargs) -> None:
        if len(kwargs) == 0: return

        _SQL = f'UPDATE {self.__table_name} SET '

        injection_list = []
        for key, val in kwargs.items():
            if key in self.column_dict:
                _SQL += f'{key} = ?,'
                injection_list.append(val)
        _SQL = _SQL[:-1]

        _SQL += ' WHERE table_id = ?'
        injection_list.append(table_id)

        self.cur.execute(_SQL, tuple(injection_list))
        self.conn.commit()

    def view_database(self) -> None:
        self.cur.execute("select * from sqlite_master where type='table'")
        for row in self.cur.fetchall():
            print(row)

    def _make_select(self, **kwargs) -> (str, list):
        SQL = f'SELECT * FROM {self.__table_name}'

        if len(kwargs) == 0:
            return SQL, []

        SQL += ' WHERE '

        injection_list = []
        key_cnt = 0
        for key, vals in kwargs.items():
            if key in self.column_dict:
                if key_cnt > 0:
                    SQL += ' AND '

                key_cnt += 1

                if not isinstance(vals, list):
                    SQL += f'{key}=? '
                    injection_list.append(vals)
                elif len(vals) == 1:
                    SQL += f'{key}=? '
                    injection_list.append(vals[0])
                else:
                    for val_cnt, val in enumerate(vals):
                        if val_cnt == 0:
                            SQL += f'({key}=? OR '
                        elif val_cnt == len(vals) - 1:
                            SQL += f'{key}=?) '
                        else:
                            SQL += f'{key}=? OR '

                        injection_list.append(val)

        return SQL, injection_list

    def view_table(self, **kwargs) -> None:
        SQL, injection_list = self._make_select(**kwargs)

        self.cur.execute(SQL, tuple(injection_list))
        for row in self.cur.fetchall():
            print(row)

    def pickup_dataframe(self, **kwargs) -> pd.DataFrame:
        SQL, injection_list = self._make_select(**kwargs)

        df = pd.read_sql(SQL, self.conn, params=tuple(injection_list))
        df.to_csv('test.csv', encoding='shift-jis', index=False)
        return df


if __name__ == '__main__':
    cols_dict = {'year': 'int',
                 'month': 'int',
                 'category': 'text',
                 'detail': 'text',
                 'value': 'int',
                 'memo': 'text'
                 }

    app = SQLApp(cols_dict)
    app.open_db_table()

    print('---------------')
    app.view_database()
    app.view_table()

    _dict = {'year': '2000',
             'month': 12,
             'category': 'ddd',
             'value': 10200
             }
    app.insert_data(**_dict)
    app.insert_data(year=2019, tyy=4)

    app.update_data(5, **{'detail': 'test---'})

    print('---------------')
    app.view_table(year=[2019, 2014])

    print('---------------')
    df = app.pickup_dataframe()
    print(df.head(100))

    app.close_db_table()
