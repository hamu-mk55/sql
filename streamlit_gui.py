import streamlit as st
from st_aggrid import AgGrid, ColumnsAutoSizeMode, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
import pandas as pd
import datetime

import plotly.express as px

from sql_control import SQLApp


class SqlDB:
    def __init__(self):
        self._sql_cols_dict = {'year': 'int',
                               'month': 'int',
                               'category': 'text',
                               'detail': 'text',
                               'value': 'int',
                               'memo': 'text'
                               }

        self._sql: SQLApp = SQLApp(self._sql_cols_dict,
                                   db_name='test.db',
                                   table_name='test')
        self.database: pd.DataFrame | None = None

        self.header: list = []
        self.columns_list: list = []
        self.items_in_col: dict[list] = {}

    def __del__(self):
        self._sql.close_db_table()

    def load_dataframe(self):
        # load dataframe
        self._sql.open_db_table()
        self.database = self._sql.pickup_dataframe()
        self._sql.close_db_table()

        # modify
        self._add_columns()

        # set params
        self.header = self.database.columns.values.tolist()
        self.columns_list.append('none')
        self.columns_list += self.header

        for column in self.columns_list:
            self.items_in_col[column] = get_items_list(self.database, key=column)

    def add_data(self, **kwargs):
        self._sql.open_db_table()

        self._sql.insert_data(**kwargs)

        self._sql.close_db_table()

    def delete_data(self, table_id:int):
        self._sql.open_db_table()

        self._sql.delete_data(table_id)

        self._sql.close_db_table()

    def update_data(self, table_id:int, **kwargs):
        self._sql.open_db_table()

        self._sql.update_data(table_id, **kwargs)

        self._sql.close_db_table()

    def _add_columns(self):
        def _int2str(x, digit_num=4):
            if digit_num == 2:
                return f'{int(x):02d}'
            else:
                return f'{int(x)}'

        year = self.database["year"].map(lambda x: _int2str(x), na_action='ignore')
        month = self.database["month"].map(lambda x: _int2str(x, digit_num=2), na_action='ignore')

        self.database["date"] = year.str.cat(month, sep='-')

    def check_params(self, param_dict:dict):
        table_id = param_dict.get('table_id', 0)

        year = param_dict.get('year', datetime.datetime.now().year)
        if year is None or type(year) != int:
            year = datetime.datetime.now().year

        month = param_dict.get('month', datetime.datetime.now().month)
        if month is None or type(month) != int:
            month = datetime.datetime.now().month

        category = param_dict.get('category', "")
        if category is None:
            category = ""

        detail = param_dict.get('detail', "")
        if detail is None:
            detail = ""

        value = param_dict.get('value', 0)
        if value is None or type(value) != int:
            value = 0

        memo = param_dict.get('memo', "")
        if memo is None:
            memo = ""

        output_dict = {'table_id': table_id,
                       'year': year,
                       'month': month,
                       'category': category,
                       'detail': detail,
                       'value': value,
                       'memo': memo
                       }
        return output_dict


def get_items_list(df: pd.DataFrame, key:str):
    item_list = []

    if key not in df.columns.values.tolist():
        return item_list

    _df = df[key].dropna()
    item_list = sorted(list(set(_df)))

    return item_list


def set_filters(db, filter_cnt=None):
    select_column = st.sidebar.selectbox(f'**filter{filter_cnt}**',
                                         db.columns_list)

    if select_column != 'none':
        select_items = st.sidebar.multiselect(f'**multi-select{filter_cnt}**',
                                              db.items_in_col[select_column],
                                              label_visibility='collapsed')
    else:
        select_items = []

    # st.sidebar.divider()
    return select_column, select_items


def set_table(db: SqlDB, _df: pd.DataFrame):
    gb = GridOptionsBuilder.from_dataframe(_df)
    gb.configure_pagination(paginationPageSize=10, paginationAutoPageSize=False)
    gb.configure_selection(selection_mode="single", use_checkbox=True)
    gridOptions = gb.build()

    data = AgGrid(_df,
                  gridOptions=gridOptions,
                  # reload_data=True,
                  # update_mode=GridUpdateMode.MANUAL,
                  columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW)

    selected = data["selected_rows"]

    if len(selected) > 0:
        def click():
            table_id = selected_dict["table_id"]
            db.delete_data(table_id)
            st.cache_resource.clear()

        st.dataframe(selected)
        selected_dict = selected[0]
        button_delete = st.button("DELETE", on_click=click)

        return selected_dict
    else:
        return {}


def set_input(db: SqlDB, _df: pd.DataFrame, input_data_dict: dict):
    col1, col2 = st.columns((1, 2))

    with col1:
        params_dict = db.check_params(input_data_dict)

        year = st.number_input('year',
                               value=params_dict['year'],
                               min_value=2000,
                               max_value=2100)
        month = st.number_input('month',
                                value=params_dict['month'],
                                min_value=1,
                                max_value=12)

    with col2:
        # category
        _category = params_dict.get("category", "")
        _category_list = db.items_in_col['category'] + ['etc']
        if _category in _category_list:
            _index = _category_list.index(_category)
        else:
            _index = len(_category_list) - 1

        category = st.selectbox('category', _category_list,
                                index=_index)
        if category == 'etc':
            category = st.text_input('text_category', label_visibility="collapsed")

        # detail
        _df = db.database
        _df = _df[_df['category'] == category]

        _detail = params_dict.get("detail", "")
        _detail_list = get_items_list(_df, key='detail') + ['etc']
        if _detail in _detail_list:
            _index = _detail_list.index(_detail)
        else:
            _index = len(_detail_list) - 1

        detail = st.selectbox('detail', _detail_list, index=_index)
        if detail == 'etc':
            detail = st.text_input('text_detail', label_visibility="collapsed")

    _val = params_dict.get("value", 0)
    val = st.number_input("value", min_value=0, value=_val)

    _memo = params_dict.get("memo", "")
    memo = st.text_input("memo", value=_memo)

    data_dict = {'year': year,
                 'month': month,
                 'category': category,
                 'detail': detail,
                 'value': val,
                 'memo': memo
                 }

    def click_add():
        db.add_data(**data_dict)
        st.cache_resource.clear()

    def click_update():
        db.update_data(input_data_dict["table_id"], **data_dict)
        st.cache_resource.clear()

    if len(input_data_dict) > 0:
        button_update = st.button("UPDATE DATA", on_click=click_update)
    else:
        button_add = st.button("ADD DATA", on_click=click_add)

def set_uploader(db):
    uploaded_file = st.file_uploader("Choose a file")
    if uploaded_file is not None:
        def click_add():
            dataframe = pd.read_csv(uploaded_file, encoding='shift-jis')

            for _, row in dataframe.iterrows():
                data_dict = {'year': row["Year"],
                             'month': row["Month"],
                             'category': row["Category"],
                             'value': row["Amount"]
                             }
                db.add_data(**data_dict)

            st.cache_resource.clear()

        button_add = st.button("ADD ALL-DATA", on_click=click_add)


def set_graph(df):
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        graph_type = st.selectbox('graph', ["bar", "circle"])


    if graph_type=='bar':
        x = 'date'
        y = 'value'
        color = 'category'

        fig = px.bar(df, x=x, y=y,
                     color=color, barmode='relative')
        fig.update_xaxes(tickformat="%Y-%m")
        st.plotly_chart(fig, use_container_width=True)
    else:
        value = 'value'
        name = 'category'

        fig = px.pie(df, values=value,
                     names=name)
        st.plotly_chart(fig, use_container_width=True)



@st.cache_resource
def load_dataframe():
    db = SqlDB()
    db.load_dataframe()
    return db


def set_streamlit():
    db = load_dataframe()
    df = db.database

    # sidebar
    filter_num = 0
    while True:
        filter_num += 1
        column, items = set_filters(db, filter_num)

        if len(items) > 0:
            df = df[df[column].isin(items)]
        else:
            break

    # main
    tab1, tab2 = st.tabs(["DATA", "GRAPH"])

    with tab1:
        col1, col2 = st.columns((3, 2))
        with col1:
            data_dict = set_table(db, df)

            set_uploader(db)
        with col2:
            set_input(db, df, data_dict)

    with tab2:
        # st.write(df)
        set_graph(df)


def main():
    st.set_page_config(layout="wide", initial_sidebar_state="auto")
    set_streamlit()


if __name__ == '__main__':
    # load_db()
    main()
