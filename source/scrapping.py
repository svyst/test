import logging
import time
from functools import partial
from multiprocessing import Pool
from urllib.parse import urlencode, urljoin

import numpy
import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def get_soup(html):
    try:
        soup = BeautifulSoup(html, features='lxml')
    except Exception:
        soup = BeautifulSoup(html, features='html.parser')
    return soup


def get_html_page(config, href):
    for i in range(config.getint('settings', 'retries_count_df')):
        try:
            r = requests.get(href)
            return r
        except Exception:
            time.sleep(int(config.get('settings', 'sleep_seconds_df')) * 5)
    else:
        logger.error(config.get('error', 'fpds_site_not_response'))
        raise Exception


def site_validation(config, fpds_href):
    r = get_html_page(config, fpds_href)
    soup = get_soup(r.text)
    if soup.find('form', {'name': 'search_awardfull'}):
        return None
    logger.error(config.get('error', 'wrong_fpds_href'))
    raise Exception


def scrape_hrefs_from_search(config, search_href):
    if not search_href:
        return []
    r = get_html_page(config, search_href)
    soup = get_soup(r.text)

    try:
        b_items = soup.find('span', {'class': 'results_heading'}).find_all_next('b')
        page_index = int(b_items[1].text)
        last_index = int(b_items[2].text)
        if last_index == 0:
            return []
        elif last_index > page_index:
            items_iter = range(0, last_index, page_index)
        else:
            items_iter = [0]
    except Exception:
        return []
    hrefs = []
    for i in items_iter:
        next_page = '{0}&{1}'.format(search_href, urlencode({'start': i}))
        r = get_html_page(config, next_page)
        soup = get_soup(r.text)

        hrefs.extend([urljoin(search_href, href['href'].replace("javascript:getParentURL('", '')
                              .replace("')", '')) for href in soup.find_all(title='View')])
    if hrefs:
        hrefs = list(set(hrefs))

    return hrefs


def get_values_from_form_page(config, mapping, href_indexed):
    index = href_indexed[0]
    href = href_indexed[1]
    mapping_values = mapping.values()
    mapping_keys = mapping.keys()
    result = {k: None for k in mapping_values}
    r = get_html_page(config, href)
    soup = get_soup(r.text)
    tds = soup.find_all('td')
    for td in tds:
        if not td.find('table'):
            if td.attrs.get('id') in mapping_values:
                result[td.attrs.get('id')] = td.text
            elif td.find('input') and td.find('input').attrs.get('id') in mapping_values:
                if td.find('input').get('value'):
                    result[td.find('input').attrs.get('id')] = td.find('input').attrs.get('value')
                elif td.find('input').get('checked'):
                    result[td.find('input').attrs.get('id')] = True
                else:
                    result[td.find('input').attrs.get('id')] = None
            elif td.find('textarea') and td.find('textarea').attrs.get('id') in mapping_values:
                result[td.find('textarea').attrs.get('id')] = td.find('textarea').text
            elif td.find('select') and td.find('select').attrs.get('id') in mapping_values:
                td_select = td.find('select').find('option', {'selected': 'true'})
                if td_select and td_select.attrs.get('value'):
                    result[td.find('select').attrs.get('id')] = td_select.text
                else:
                    result[td.find('select').attrs.get('id')] = None
    form = [result.get(mapping.get(k, None)) for k in mapping_keys]
    if [v for v in form if v != None]:
        return index, [result.get(mapping.get(k, None)) for k in mapping_keys]
    return index, None


def get_forms_df(config, search_names, mapping):
    site_validation(config, config.get('settings', 'search_HREF'))
    try:
        logger.info(config.get('info', 'parse_search_names').format(len(search_names)))
        page_searchs = [('{0}?{1}'.format(config.get('settings', 'search_HREF'), urlencode({'q': name}))
                         if name.replace(' ', '') else '') for name in search_names]
        if len(page_searchs) > int(config.get('settings', 'pool')) * 5:
            page_searchs_list = numpy.array_split(page_searchs,
                                                  len(page_searchs) // (int(config.get('settings', 'pool')) * 5))
        else:
            page_searchs_list = [page_searchs]
        view_hrefs_list = []
        func = partial(scrape_hrefs_from_search, config)
        for i, page_search in enumerate(page_searchs_list):
            with Pool(int(config.get('settings', 'pool'))) as p:
                view_hrefs_list.extend(p.map(func, page_search))

        view_hrefs = [[i, y] for i, x in enumerate(view_hrefs_list) for y in x]
        names_success = [bool(i) for i in view_hrefs_list]

        logger.info(config.get('info', 'parse_forms').format(len(view_hrefs)))
        if len(view_hrefs) > (int(config.get('settings', 'pool')) * 5):
            view_hrefs = numpy.array_split(view_hrefs, len(view_hrefs) // (int(config.get('settings', 'pool')) * 5))
        else:
            view_hrefs = [view_hrefs]
        result = []
        non_processed = []
        func = partial(get_values_from_form_page, config, mapping)
        for hrefs in view_hrefs:
            with Pool(int(config.get('settings', 'pool'))) as p:
                result_loc = p.map(func, hrefs)
                for i, form in result_loc:
                    if form is None:
                        non_processed.append(i)
                    else:
                        result.append(form)

        for i in non_processed:
            names_success[i] = False
        if len(names_success) == 0:
            return {'status_res': names_success, 'output_df': pd.DataFrame}
        df = pd.DataFrame.from_records(result)
        df.columns = mapping.keys()
        return {'status_res': names_success, 'output_df': df}

    except Exception as e:
        logger.exception(e)
        raise e
