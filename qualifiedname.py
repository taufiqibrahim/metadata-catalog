from atlasclient.utils import parse_table_qualified_name
import re

def parse_column_qualified_name(qualified_name):
    qn_regex = re.compile(r"""
        ^(?P<db_name>.*?)\.(?P<table_name>.*)\.(?P<column_name>.*)@(?P<cluster_name>.*?)$
        """, re.X)

    def apply_qn_regex(name, column_qn_regex):
        return column_qn_regex.match(name)

    _regex_result = apply_qn_regex(qualified_name, qn_regex)

    if not _regex_result:
        qn_regex = re.compile(r"""
        ^(?P<table_name>.*?)\.(?P<column_name>.*)@(?P<cluster_name>.*?)$
        """, re.X)
        _regex_result = apply_qn_regex(qualified_name, qn_regex)

    if not _regex_result:
        qn_regex = re.compile(r"""
        ^(?P<column_name>.*)@(?P<cluster_name>.*?)$
        """, re.X)
        _regex_result = apply_qn_regex(qualified_name, qn_regex)

    if not _regex_result:
        qn_regex = re.compile(r"""
        ^(?P<column_name>.*)$
        """, re.X)
        _regex_result = apply_qn_regex(qualified_name, qn_regex)

    _regex_result = _regex_result.groupdict()

    qn_dict = {
        'column_name': _regex_result.get('column_name', qualified_name),
        'table_name': _regex_result.get('table_name', "default"),
        'db_name': _regex_result.get('db_name', "default"),
        'cluster_name': _regex_result.get('cluster_name', "default"),
    }

    return qn_dict

x = 'sakila.customer.address@clx'
print(parse_column_qualified_name(x))

table = 'sakila.customer@clx'
print(parse_table_qualified_name(table))