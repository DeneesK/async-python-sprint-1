import os
from os.path import exists

import pytest


@pytest.mark.parametrize(
    'checking_city, expected_answer',
    [
        (
            ['MOSCOW'],
            {'city': 'MOSCOW', }
        )
    ]
)
def test_data_fetching(data_fetch, checking_city, expected_answer):
    data = data_fetch(checking_city)

    assert data[0].city == expected_answer['city']


def test_calc_data(calc_data):
    calculated_data = calc_data

    assert calculated_data.city == 'MOSCOW'
    assert calculated_data.dates[0].avg_temp == 13.1
    assert calculated_data.dates[0].good_hours == 11


def test_agtegate_data(agregate_data):
    agregate_data()

    file_exists = exists('tests\\test.xlsx')  # for Linux 'tests/test.json'
    assert file_exists


def test_analyz(analyz_data):
    result = analyz_data()

    file_exists = exists('tests\\test.xlsx')  # for Linux 'tests/test.json'
    assert file_exists
    assert result == '*****Recommended city/ies to visit "MOSCOW"*****'

    os.remove('tests\\test.xlsx')
